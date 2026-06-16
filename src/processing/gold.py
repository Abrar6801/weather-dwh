"""
Gold Layer: Silver → Aggregated analytical tables.
FIX v3: Fully implemented with Delta format (was missing entirely).
All writes use Delta MERGE or overwrite — no plain Parquet.
"""

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.processing.spark_session import get_spark
from src.security.secrets import get_settings

logger = logging.getLogger(__name__)


class GoldProcessor:
    """
    Builds four Gold Delta tables from Silver:
      1. city_daily_summary    — one row per city per day
      2. global_hourly_trends  — average metrics by local hour
      3. country_rankings      — country-level ranked stats
      4. extreme_events        — observations exceeding alert thresholds
    """

    def __init__(self, spark: SparkSession = None) -> None:
        self.spark = spark or get_spark("WeatherGold")
        settings = get_settings()
        self.silver_path = settings.silver_data_path
        self.gold_path = Path(settings.gold_data_path)
        self.gold_path.mkdir(parents=True, exist_ok=True)

    def read_silver(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.silver_path)

    # ── Table 1: City Daily Summary ──────────────────────────────────────────

    def city_daily_summary(self, df: DataFrame) -> DataFrame:
        """
        Grain: (city_id, date).
        Aggregates all hourly observations into one daily row per city.
        """
        return (
            df
            .withColumn("obs_date", F.to_date("observed_at"))
            .groupBy("city_id", "city_name", "country", "obs_date")
            .agg(
                F.round(F.avg("temp_celsius"), 2).alias("avg_temp_celsius"),
                F.round(F.min("temp_celsius"), 2).alias("min_temp_celsius"),
                F.round(F.max("temp_celsius"), 2).alias("max_temp_celsius"),
                F.round(
                    F.max("temp_celsius") - F.min("temp_celsius"), 2
                ).alias("temp_range_celsius"),
                F.round(F.stddev("temp_celsius"), 3).alias("stddev_temp"),
                F.round(F.avg("feels_like_celsius"), 2).alias("avg_feels_like_celsius"),
                F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
                F.round(F.max("humidity_pct"), 0).alias("max_humidity_pct"),
                F.round(F.avg("wind_speed_ms"), 2).alias("avg_wind_speed_ms"),
                F.round(F.max("wind_speed_ms"), 2).alias("max_wind_speed_ms"),
                F.round(F.max("wind_gust_ms"), 2).alias("max_gust_ms"),
                F.round(F.avg("pressure_hpa"), 1).alias("avg_pressure_hpa"),
                F.round(F.avg("visibility_m"), 0).alias("avg_visibility_m"),
                F.round(F.avg("cloud_cover_pct"), 1).alias("avg_cloud_cover_pct"),
                F.first("weather_main").alias("dominant_weather"),
                F.first("daylight_minutes").alias("daylight_minutes"),
                F.round(F.avg("dq_score"), 1).alias("avg_dq_score"),
                F.count("*").alias("observation_count"),
            )
            .withColumn("year",  F.year("obs_date"))
            .withColumn("month", F.month("obs_date"))
        )

    # ── Table 2: Global Hourly Trends ────────────────────────────────────────

    def global_hourly_trends(self, df: DataFrame) -> DataFrame:
        """
        Grain: local_hour (0–23).
        Cross-city average by hour — answers "what time of day is warmest globally?".
        """
        return (
            df
            .groupBy("local_hour")
            .agg(
                F.round(F.avg("temp_celsius"), 2).alias("avg_temp_celsius"),
                F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
                F.round(F.avg("wind_speed_ms"), 2).alias("avg_wind_speed_ms"),
                F.round(F.avg("cloud_cover_pct"), 1).alias("avg_cloud_cover_pct"),
                F.count("*").alias("sample_count"),
            )
            .orderBy("local_hour")
        )

    # ── Table 3: Country Rankings ────────────────────────────────────────────

    def country_rankings(self, df: DataFrame) -> DataFrame:
        """
        Grain: country.
        Ranked by average temperature, wind, and humidity using window functions.
        """
        agg = (
            df
            .groupBy("country")
            .agg(
                F.round(F.avg("temp_celsius"), 2).alias("avg_temp"),
                F.round(F.avg("humidity_pct"), 1).alias("avg_humidity"),
                F.round(F.avg("wind_speed_ms"), 2).alias("avg_wind"),
                F.round(F.avg("cloud_cover_pct"), 1).alias("avg_clouds"),
                F.countDistinct("city_id").alias("city_count"),
                F.count("*").alias("observation_count"),
            )
        )

        return (
            agg
            .withColumn("temp_rank",     F.rank().over(Window.orderBy(F.col("avg_temp").desc())))
            .withColumn("wind_rank",     F.rank().over(Window.orderBy(F.col("avg_wind").desc())))
            .withColumn("humidity_rank", F.rank().over(Window.orderBy(F.col("avg_humidity").desc())))
        )

    # ── Table 4: Extreme Events ──────────────────────────────────────────────

    def extreme_events(self, df: DataFrame) -> DataFrame:
        """
        Grain: individual observation exceeding alert thresholds.
        Tagged with event_type for downstream alerting.
        """
        return (
            df
            .filter(
                (F.col("temp_celsius") >= 40)
                | (F.col("temp_celsius") <= -20)
                | (F.col("wind_speed_ms") >= 24.5)
                | (F.col("humidity_pct") >= 95)
                | (F.col("visibility_m") <= 200)
            )
            .withColumn(
                "event_type",
                F.when(F.col("temp_celsius") >= 40, "extreme_heat")
                 .when(F.col("temp_celsius") <= -20, "extreme_cold")
                 .when(F.col("wind_speed_ms") >= 24.5, "storm_wind")
                 .when(F.col("humidity_pct") >= 95, "saturation_humidity")
                 .otherwise("low_visibility"),
            )
            .select(
                "city_id", "city_name", "country", "observed_at",
                "temp_celsius", "wind_speed_ms", "humidity_pct",
                "visibility_m", "weather_main", "weather_description",
                "wind_category", "temp_category", "event_type",
            )
            .orderBy(F.col("observed_at").desc())
        )

    # ── Write helper ─────────────────────────────────────────────────────────

    def _write_delta(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        path = str(self.gold_path / table_name)
        df.write.format("delta").mode(mode).save(path)
        logger.info("Gold table '%s' written → %s (%d rows)", table_name, path, df.count())

    # ── Orchestrate ──────────────────────────────────────────────────────────

    def run(self, input_df: DataFrame = None) -> dict[str, DataFrame]:
        """Full Gold job. Returns dict of table_name → DataFrame."""
        df = input_df if input_df is not None else self.read_silver()
        tables = {
            "city_daily_summary":   self.city_daily_summary(df),
            "global_hourly_trends": self.global_hourly_trends(df),
            "country_rankings":     self.country_rankings(df),
            "extreme_events":       self.extreme_events(df),
        }
        for name, tbl in tables.items():
            self._write_delta(tbl, name)
        logger.info("Gold done. Tables: %s", list(tables.keys()))
        return tables
