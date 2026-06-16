"""
Fact: Daily weather summary.
FIX v3: Fully implemented (was missing in v2).
Grain: one row per (city, date). Pre-aggregated from hourly observations.
"""

import logging

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.processing.spark_session import get_spark
from src.security.secrets import get_settings

logger = logging.getLogger(__name__)


class FactDailyWeatherLoader:

    def __init__(self, spark: SparkSession = None) -> None:
        self.spark = spark or get_spark("FactDailyWeather")
        settings = get_settings()
        self.silver_path = settings.silver_data_path
        self.fact_path = settings.warehouse_data_path + "/fact_daily_weather"
        self.dim_city_path = settings.warehouse_data_path + "/dim_city"

    def build(self, silver_df: DataFrame) -> DataFrame:
        dim_city = (
            self.spark.read.format("delta").load(self.dim_city_path)
            .filter(F.col("is_current"))
            .select("city_sk", "city_id")
        )
        return (
            silver_df
            .join(dim_city, on="city_id", how="left")
            .withColumn("obs_date", F.to_date("observed_at"))
            .withColumn("date_key", F.date_format("observed_at", "yyyyMMdd").cast("int"))
            .groupBy("city_sk", "city_id", "city_name", "country", "obs_date", "date_key")
            .agg(
                F.round(F.avg("temp_celsius"), 2).alias("avg_temp_celsius"),
                F.round(F.min("temp_celsius"), 2).alias("min_temp_celsius"),
                F.round(F.max("temp_celsius"), 2).alias("max_temp_celsius"),
                F.round(F.stddev("temp_celsius"), 3).alias("stddev_temp"),
                F.round(F.avg("feels_like_celsius"), 2).alias("avg_feels_like_celsius"),
                F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
                F.round(F.max("humidity_pct"), 0).alias("max_humidity_pct"),
                F.round(F.avg("wind_speed_ms"), 2).alias("avg_wind_speed_ms"),
                F.round(F.max("wind_speed_ms"), 2).alias("max_wind_speed_ms"),
                F.round(F.avg("pressure_hpa"), 1).alias("avg_pressure_hpa"),
                F.round(F.avg("visibility_m"), 0).alias("avg_visibility_m"),
                F.round(F.avg("cloud_cover_pct"), 1).alias("avg_cloud_cover_pct"),
                F.first("weather_main").alias("dominant_condition"),
                F.first("daylight_minutes").alias("daylight_minutes"),
                F.round(F.avg("dq_score"), 1).alias("avg_dq_score"),
                F.count("*").alias("observation_count"),
            )
            .withColumn("year",  F.year("obs_date"))
            .withColumn("month", F.month("obs_date"))
        )

    def load(self, silver_df: DataFrame = None) -> DataFrame:
        df = silver_df or self.spark.read.format("delta").load(self.silver_path)
        fact_df = self.build(df)
        path = self.fact_path
        if DeltaTable.isDeltaTable(self.spark, path):
            tbl = DeltaTable.forPath(self.spark, path)
            (
                tbl.alias("f")
                .merge(
                    fact_df.alias("n"),
                    "f.city_id = n.city_id AND f.date_key = n.date_key",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            fact_df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(path)

        logger.info("fact_daily_weather loaded: %d rows", fact_df.count())
        return fact_df
