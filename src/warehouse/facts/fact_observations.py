"""
Fact: Hourly weather observations.
Grain: one row per (city, observed_at hour).
Resolves surrogate keys from dimension tables before loading.
"""

import logging

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

FACT_SCHEMA = StructType([
    StructField("observation_sk",      StringType(),   False),  # UUID
    StructField("city_sk",             StringType(),   False),  # FK dim_city
    StructField("date_key",            IntegerType(),  False),  # FK dim_date
    StructField("time_key",            IntegerType(),  False),  # FK dim_time
    StructField("condition_id",        IntegerType(),  True),   # FK dim_weather
    StructField("city_id",             LongType(),     False),  # Degenerate dim
    StructField("observed_at",         TimestampType(),False),
    StructField("pipeline_run_id",     StringType(),   True),
    StructField("temp_celsius",        DoubleType(),   True),
    StructField("feels_like_celsius",  DoubleType(),   True),
    StructField("temp_min_celsius",    DoubleType(),   True),
    StructField("temp_max_celsius",    DoubleType(),   True),
    StructField("heat_index_celsius",  DoubleType(),   True),
    StructField("pressure_hpa",        IntegerType(),  True),
    StructField("humidity_pct",        IntegerType(),  True),
    StructField("visibility_m",        IntegerType(),  True),
    StructField("wind_speed_ms",       DoubleType(),   True),
    StructField("wind_deg",            IntegerType(),  True),
    StructField("wind_gust_ms",        DoubleType(),   True),
    StructField("cloud_cover_pct",     IntegerType(),  True),
    StructField("is_daytime",          BooleanType(),  True),
    StructField("dq_score",            IntegerType(),  True),
    StructField("year",                IntegerType(),  False),
    StructField("month",               IntegerType(),  False),
])


class FactObservationsLoader:

    def __init__(self, spark: SparkSession = None) -> None:
        self.spark = spark or get_spark("FactObservations")
        settings = get_settings()
        self.silver_path = settings.silver_data_path
        self.fact_path = settings.warehouse_data_path + "/fact_observations"
        self.dim_city_path = settings.warehouse_data_path + "/dim_city"

    def resolve_keys(self, df: DataFrame) -> DataFrame:
        """Join dim_city to get city_sk, derive date_key and time_key."""
        dim_city = (
            self.spark.read.format("delta").load(self.dim_city_path)
            .filter(F.col("is_current"))
            .select("city_sk", "city_id")
        )
        return (
            df
            .join(dim_city, on="city_id", how="left")
            .withColumn("date_key",
                F.date_format("observed_at", "yyyyMMdd").cast(IntegerType()))
            .withColumn("time_key",
                (F.hour("observed_at") * 100 + F.minute("observed_at")).cast(IntegerType()))
            .withColumn("year",  F.year("observed_at"))
            .withColumn("month", F.month("observed_at"))
            .withColumn("observation_sk", F.expr("uuid()"))
            .withColumn("condition_id", F.col("weather_condition_id"))
        )

    def load(self, silver_df: DataFrame = None) -> DataFrame:
        df = silver_df or self.spark.read.format("delta").load(self.silver_path)
        fact_df = self.resolve_keys(df)
        available = set(fact_df.columns)
        cols = [f.name for f in FACT_SCHEMA.fields if f.name in available]
        fact_df = fact_df.select(cols)

        path = self.fact_path
        if DeltaTable.isDeltaTable(self.spark, path):
            tbl = DeltaTable.forPath(self.spark, path)
            (
                tbl.alias("f")
                .merge(
                    fact_df.alias("n"),
                    "f.city_id = n.city_id AND f.observed_at = n.observed_at",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            fact_df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(path)

        logger.info("fact_observations loaded: %d rows", fact_df.count())
        return fact_df
