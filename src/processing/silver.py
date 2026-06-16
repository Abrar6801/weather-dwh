"""
Silver Layer: Bronze → Cleaned, validated, enriched.
CDC watermark is now PostgreSQL-backed (survives between GitHub Actions runs).
Heat index uses full 9-term Rothfusz regression (FIX v3).
"""

import logging
from datetime import UTC, datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.cdc.watermark import WatermarkManager
from src.processing.spark_session import get_spark
from src.security.secrets import get_settings

logger = logging.getLogger(__name__)

TEMP_MIN = -89.2
TEMP_MAX = 56.7
WIND_MAX = 113.0


class SilverProcessor:
    """Reads incremental Bronze records and writes cleaned Silver Delta table."""

    def __init__(self, spark: SparkSession = None) -> None:
        self.spark = spark or get_spark("WeatherSilver")
        settings = get_settings()
        self.bronze_path = Path(settings.bronze_data_path)
        self.silver_path = Path(settings.silver_data_path)
        self.quarantine_path = Path(settings.quarantine_data_path)
        self.silver_path.mkdir(parents=True, exist_ok=True)
        self.quarantine_path.mkdir(parents=True, exist_ok=True)
        self.watermark = WatermarkManager(layer="silver")

    # ── CDC ──────────────────────────────────────────────────────────────────

    def read_incremental(self, input_df: DataFrame = None) -> DataFrame:
        """
        Read only Bronze records newer than the last watermark.
        FIX v3: bronze_loaded_at is TimestampType so comparison is type-safe.
        """
        if input_df is not None:
            return input_df

        df = self.spark.read.format("delta").load(str(self.bronze_path))
        last_mark = self.watermark.get_last_watermark()
        if last_mark:
            df = df.filter(F.col("bronze_loaded_at") > F.lit(last_mark).cast("timestamp"))
            logger.info("CDC: processing Bronze records after %s", last_mark)
        else:
            logger.info("CDC: no watermark — full Bronze load")
        return df

    # ── Deduplication ────────────────────────────────────────────────────────

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """Keep the most recently loaded record per (city_id, observed_at_utc)."""
        w = Window.partitionBy("city_id", "observed_at_utc").orderBy(
            F.col("bronze_loaded_at").desc()
        )
        return (
            df.withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

    # ── Data Quality Split ───────────────────────────────────────────────────

    def split_valid_quarantine(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Route records: valid → Silver, failed DQ → quarantine Delta table.
        Returns (valid_df, quarantine_df).
        """
        failed = (
            F.col("city_name").isNull()
            | F.col("temp_celsius").isNull()
            | F.col("observed_at_utc").isNull()
            | ~F.col("temp_celsius").between(TEMP_MIN, TEMP_MAX)
            | ~F.col("humidity_pct").between(0, 100)
        )

        quarantine_df = (
            df.filter(failed)
            .withColumn(
                "quarantine_reason",
                F.when(F.col("city_name").isNull(), "null_city_name")
                 .when(F.col("temp_celsius").isNull(), "null_temperature")
                 .when(F.col("observed_at_utc").isNull(), "null_observed_at")
                 .when(~F.col("temp_celsius").between(TEMP_MIN, TEMP_MAX), "temp_out_of_range")
                 .otherwise("humidity_out_of_range"),
            )
            .withColumn(
                "quarantined_at",
                F.lit(datetime.now(tz=UTC).isoformat()).cast("timestamp"),
            )
        )

        valid_df = df.filter(~failed)
        logger.info(
            "DQ split — valid: %d | quarantined: %d",
            valid_df.count(),
            quarantine_df.count(),
        )
        return valid_df, quarantine_df

    def write_quarantine(self, df: DataFrame) -> None:
        if df.rdd.isEmpty():
            return
        df.write.format("delta").mode("append").save(str(self.quarantine_path))
        logger.warning("Quarantined %d records", df.count())

    # ── Transformations ──────────────────────────────────────────────────────

    def clamp_outliers(self, df: DataFrame) -> DataFrame:
        """Null out physically impossible values."""
        return (
            df
            .withColumn(
                "temp_celsius",
                F.when(F.col("temp_celsius").between(TEMP_MIN, TEMP_MAX), F.col("temp_celsius"))
                 .otherwise(F.lit(None)),
            )
            .withColumn(
                "wind_speed_ms",
                F.when(F.col("wind_speed_ms") <= WIND_MAX, F.col("wind_speed_ms"))
                 .otherwise(F.lit(None)),
            )
        )

    def parse_timestamps(self, df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("observed_at", F.to_timestamp("observed_at_utc"))
            .withColumn("ingested_at", F.to_timestamp("ingested_at_utc"))
            .withColumn("sunrise", F.to_timestamp("sunrise_utc"))
            .withColumn("sunset", F.to_timestamp("sunset_utc"))
        )

    def enrich(self, df: DataFrame) -> DataFrame:
        """
        Derive analytical columns.
        FIX v3: Full 9-term Rothfusz heat index equation (was simplified/inaccurate).
        """
        T = F.col("temp_celsius")
        RH = F.col("humidity_pct")

        full_heat_index = (
            -8.78469475556
            + 1.61139411 * T
            + 2.33854883889 * RH
            - 0.14611605 * T * RH
            - 0.012308094 * T * T
            - 0.0164248277778 * RH * RH
            + 0.002211732 * T * T * RH
            + 0.00072546 * T * RH * RH
            - 0.000003582 * T * T * RH * RH
        )

        return (
            df
            .withColumn(
                "temp_category",
                F.when(T < 0, "freezing")
                 .when(T < 10, "cold")
                 .when(T < 20, "mild")
                 .when(T < 30, "warm")
                 .otherwise("hot"),
            )
            .withColumn(
                "wind_category",
                F.when(F.col("wind_speed_ms") < 0.5, "calm")
                 .when(F.col("wind_speed_ms") < 5.5, "light")
                 .when(F.col("wind_speed_ms") < 13.9, "moderate")
                 .when(F.col("wind_speed_ms") < 24.5, "strong")
                 .otherwise("storm"),
            )
            .withColumn(
                "local_hour",
                ((F.hour("observed_at") + F.col("timezone_offset_sec") / 3600)
                 .cast("int") % 24),
            )
            .withColumn(
                "daylight_minutes",
                F.round(
                    (F.unix_timestamp("sunset") - F.unix_timestamp("sunrise")) / 60, 0
                ).cast("int"),
            )
            .withColumn(
                "is_daytime",
                F.col("observed_at").between(F.col("sunrise"), F.col("sunset")),
            )
            # Full Rothfusz equation (valid when temp >= 27°C and humidity >= 40%)
            .withColumn(
                "heat_index_celsius",
                F.when(
                    (T >= 27) & RH.isNotNull() & (RH >= 40),
                    F.round(full_heat_index, 1),
                ).otherwise(F.lit(None)),
            )
            .withColumn(
                "dq_score",
                (
                    F.lit(100)
                    - F.when(F.col("visibility_m").isNull(), 15).otherwise(0)
                    - F.when(F.col("wind_gust_ms").isNull(), 10).otherwise(0)
                    - F.when(F.col("heat_index_celsius").isNull(), 5).otherwise(0)
                ),
            )
            .withColumn("city_name", F.initcap(F.trim(F.col("city_name"))))
            .withColumn("country", F.upper(F.trim(F.col("country"))))
            .withColumn("weather_description", F.lower(F.trim(F.col("weather_description"))))
        )

    # ── Write ────────────────────────────────────────────────────────────────

    def merge_silver(self, df: DataFrame) -> None:
        """Idempotent MERGE on (city_id, observed_at)."""
        path = str(self.silver_path)
        if DeltaTable.isDeltaTable(self.spark, path):
            tbl = DeltaTable.forPath(self.spark, path)
            (
                tbl.alias("s")
                .merge(
                    df.alias("n"),
                    "s.city_id = n.city_id AND s.observed_at = n.observed_at",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            df.write.format("delta").mode("overwrite").partitionBy("country").save(path)

    # ── Orchestrate ──────────────────────────────────────────────────────────

    def run(self, input_df: DataFrame = None) -> DataFrame:
        """Full Silver job. Watermark updated only after successful write."""
        df = self.read_incremental(input_df)
        df = self.deduplicate(df)
        valid_df, quarantine_df = self.split_valid_quarantine(df)
        self.write_quarantine(quarantine_df)
        df = self.clamp_outliers(valid_df)
        df = self.parse_timestamps(df)
        df = self.enrich(df)
        self.merge_silver(df)
        self.watermark.update_watermark()   # Only reaches here on success
        logger.info("Silver done. Valid rows: %d", df.count())
        return df
