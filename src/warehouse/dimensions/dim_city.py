"""
City Dimension — SCD Type 2.
FIX v3: Replaced .collect() anti-pattern with a distributed join.
No data is brought to the driver for the change detection step.
"""

import logging
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

from src.processing.spark_session import get_spark
from src.security.secrets import get_settings

logger = logging.getLogger(__name__)

SCD2_SCHEMA = StructType([
    StructField("city_sk",         StringType(),   False),   # UUID surrogate key
    StructField("city_id",         LongType(),     False),   # Natural key
    StructField("city_name",       StringType(),   False),
    StructField("country",         StringType(),   True),
    StructField("latitude",        DoubleType(),   True),
    StructField("longitude",       DoubleType(),   True),
    StructField("timezone_offset", IntegerType(),  True),
    StructField("effective_from",  TimestampType(),False),
    StructField("effective_to",    TimestampType(),True),    # NULL = current
    StructField("is_current",      BooleanType(),  False),
    StructField("row_hash",        StringType(),   False),   # MD5 of tracked cols
])

_TRACKED = ["city_name", "country", "timezone_offset"]


def _add_hash(df: DataFrame) -> DataFrame:
    """MD5 of tracked columns — change detection without bring data to driver."""
    concat_expr = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in _TRACKED],
    )
    return df.withColumn("row_hash", F.md5(concat_expr))


class DimCityProcessor:
    """
    SCD Type 2 processor.

    Algorithm (fully distributed — no .collect()):
      1. Hash incoming records on tracked columns
      2. LEFT JOIN with current dim rows (is_current=True)
      3. Flag rows where hash changed or city is new
      4. Use Delta MERGE to:
         a. Expire old rows (effective_to = now, is_current = False)
         b. Insert new rows with new surrogate keys
    """

    def __init__(self, spark: SparkSession = None) -> None:
        self.spark = spark or get_spark("DimCity")
        settings = get_settings()
        self.dim_path = settings.warehouse_data_path + "/dim_city"

    def _now_ts(self) -> str:
        return datetime.now(tz=timezone.utc).isoformat()

    def _prepare_incoming(self, source_df: DataFrame) -> DataFrame:
        """Add hash and select only the columns we track."""
        return _add_hash(
            source_df.select(
                "city_id", "city_name", "country",
                "latitude", "longitude",
                F.col("timezone_offset_sec").alias("timezone_offset"),
            )
        )

    def initialize(self, source_df: DataFrame) -> None:
        """First-time load — all cities are current."""
        now = self._now_ts()
        df = (
            self._prepare_incoming(source_df)
            .withColumn("city_sk",       F.expr("uuid()"))
            .withColumn("effective_from",F.lit(now).cast(TimestampType()))
            .withColumn("effective_to",  F.lit(None).cast(TimestampType()))
            .withColumn("is_current",    F.lit(True))
            .select([f.name for f in SCD2_SCHEMA.fields])
        )
        df.write.format("delta").mode("overwrite").save(self.dim_path)
        logger.info("dim_city initialized with %d cities", df.count())

    def apply_scd2(self, source_df: DataFrame) -> None:
        """
        Apply SCD2 changes using a fully distributed Delta MERGE.
        FIX v3: No .collect() — change detection via broadcast join.
        """
        if not DeltaTable.isDeltaTable(self.spark, self.dim_path):
            self.initialize(source_df)
            return

        now = self._now_ts()
        incoming = self._prepare_incoming(source_df)

        dim_current = (
            self.spark.read.format("delta").load(self.dim_path)
            .filter(F.col("is_current"))
            .select("city_id", "city_sk", "row_hash")
        )

        # FIX: Distributed join — change detection stays in Spark, not driver
        changes = (
            incoming.alias("new")
            .join(dim_current.alias("old"), on="city_id", how="left")
            .filter(
                F.col("old.city_id").isNull()                    # New city
                | (F.col("new.row_hash") != F.col("old.row_hash"))  # Attribute changed
            )
            .select("new.*")
        )

        has_changes = not changes.rdd.isEmpty()
        if not has_changes:
            logger.info("SCD2: no changes detected for dim_city")
            return

        # Step 1: Expire old current rows for changed cities using MERGE
        dim_table = DeltaTable.forPath(self.spark, self.dim_path)
        (
            dim_table.alias("dim")
            .merge(
                changes.alias("chg"),
                "dim.city_id = chg.city_id AND dim.is_current = true",
            )
            .whenMatchedUpdate(set={
                "effective_to": F.lit(now).cast(TimestampType()),
                "is_current":   F.lit(False),
            })
            .execute()
        )

        # Step 2: Insert new current rows for changed/new cities
        new_rows = (
            changes
            .withColumn("city_sk",       F.expr("uuid()"))
            .withColumn("effective_from",F.lit(now).cast(TimestampType()))
            .withColumn("effective_to",  F.lit(None).cast(TimestampType()))
            .withColumn("is_current",    F.lit(True))
            .select([f.name for f in SCD2_SCHEMA.fields])
        )
        new_rows.write.format("delta").mode("append").save(self.dim_path)
        logger.info("SCD2: %d new/changed city rows inserted", new_rows.count())
