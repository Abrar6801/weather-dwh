"""
Bronze Layer: flat dicts from API → typed Delta Lake table.
No business logic — schema enforcement and idempotent MERGE only.

FIX v3: Consistent partitioning on both first-run and MERGE paths.
FIX v3: pipeline_run_id and bronze_loaded_at stored as correct types.
"""

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.processing.spark_session import get_spark
from src.security.secrets import get_settings

logger = logging.getLogger(__name__)

BRONZE_SCHEMA = StructType([
    StructField("city_id",              LongType(),      False),
    StructField("city_name",            StringType(),    False),
    StructField("country",              StringType(),    True),
    StructField("latitude",             DoubleType(),    True),
    StructField("longitude",            DoubleType(),    True),
    StructField("temp_celsius",         DoubleType(),    True),
    StructField("feels_like_celsius",   DoubleType(),    True),
    StructField("temp_min_celsius",     DoubleType(),    True),
    StructField("temp_max_celsius",     DoubleType(),    True),
    StructField("pressure_hpa",         IntegerType(),   True),
    StructField("humidity_pct",         IntegerType(),   True),
    StructField("visibility_m",         IntegerType(),   True),
    StructField("wind_speed_ms",        DoubleType(),    True),
    StructField("wind_deg",             IntegerType(),   True),
    StructField("wind_gust_ms",         DoubleType(),    True),
    StructField("cloud_cover_pct",      IntegerType(),   True),
    StructField("weather_condition_id", IntegerType(),   True),
    StructField("weather_main",         StringType(),    True),
    StructField("weather_description",  StringType(),    True),
    StructField("weather_icon",         StringType(),    True),
    StructField("sunrise_utc",          StringType(),    True),
    StructField("sunset_utc",           StringType(),    True),
    # FIX: stored as TimestampType so CDC watermark comparison is type-safe
    StructField("observed_at_utc",      TimestampType(), False),
    StructField("ingested_at_utc",      TimestampType(), True),
    StructField("bronze_loaded_at",     TimestampType(), True),
    StructField("timezone_offset_sec",  IntegerType(),   True),
    StructField("pipeline_run_id",      StringType(),    True),
])


class BronzeProcessor:
    """Loads raw weather records into the Bronze Delta Lake table."""

    def __init__(self, spark: SparkSession = None) -> None:
        self.spark = spark or get_spark("WeatherBronze")
        settings = get_settings()
        self.bronze_path = Path(settings.bronze_data_path)
        self.bronze_path.mkdir(parents=True, exist_ok=True)

    def _records_to_df(self, records: list[dict]) -> DataFrame:
        """Convert flat dicts to DataFrame with metadata columns."""
        run_id = str(uuid.uuid4())
        now = datetime.now(tz=timezone.utc).isoformat()
        enriched = [
            {**r, "pipeline_run_id": run_id, "bronze_loaded_at": now}
            for r in records
        ]
        return self.spark.createDataFrame(enriched)

    def _cast_schema(self, df: DataFrame) -> DataFrame:
        """Cast all columns to canonical BRONZE_SCHEMA types."""
        casted = []
        for field in BRONZE_SCHEMA.fields:
            if field.name in df.columns:
                casted.append(F.col(field.name).cast(field.dataType).alias(field.name))
            else:
                casted.append(F.lit(None).cast(field.dataType).alias(field.name))
        return df.select(casted)

    def _merge_or_create(self, df: DataFrame) -> None:
        """
        Idempotent write:
        - Existing Delta table → MERGE on (city_id, observed_at_utc)
        - First run → create table WITH partitioning (consistent with MERGE path)

        FIX v3: First-run path now uses partitionBy("country") matching MERGE path.
        """
        path = str(self.bronze_path)
        if DeltaTable.isDeltaTable(self.spark, path):
            tbl = DeltaTable.forPath(self.spark, path)
            (
                tbl.alias("existing")
                .merge(
                    df.alias("incoming"),
                    "existing.city_id = incoming.city_id "
                    "AND existing.observed_at_utc = incoming.observed_at_utc",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("Bronze MERGE complete")
        else:
            # FIX: partitionBy on first run — consistent with subsequent MERGEs
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .partitionBy("country")
                .save(path)
            )
            logger.info("Bronze Delta table created → %s", path)

    def run(self, records: list[dict]) -> DataFrame:
        """Full Bronze job. Returns the typed DataFrame for chaining."""
        raw_df = self._records_to_df(records)
        typed_df = self._cast_schema(raw_df)
        self._merge_or_create(typed_df)
        logger.info("Bronze done. Rows: %d", typed_df.count())
        return typed_df
