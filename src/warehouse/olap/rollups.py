"""
OLAP aggregations: ROLLUP, CUBE, GROUPING SETS, gaps-and-islands streak detection.
Used for advanced analytical queries via the analytics API router.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class OLAPAnalytics:
    """Advanced OLAP operations on the Silver or Gold layer DataFrames."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def temperature_rollup(self, df: DataFrame) -> DataFrame:
        """
        ROLLUP: (country, city_name, obs_date) → subtotals at each level.
        Enables drill-down from global → country → city → day.
        """
        return (
            df
            .withColumn("obs_date", F.to_date("observed_at"))
            .rollup("country", "city_name", "obs_date")
            .agg(
                F.round(F.avg("temp_celsius"), 2).alias("avg_temp_celsius"),
                F.round(F.min("temp_celsius"), 2).alias("min_temp_celsius"),
                F.round(F.max("temp_celsius"), 2).alias("max_temp_celsius"),
                F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
                F.count("*").alias("row_count"),
            )
            .withColumn(
                "level",
                F.when(F.col("country").isNull(), "global")
                 .when(F.col("city_name").isNull(), "country")
                 .when(F.col("obs_date").isNull(), "city")
                 .otherwise("day"),
            )
        )

    def weather_cube(self, df: DataFrame) -> DataFrame:
        """
        CUBE: all combinations of (country, temp_category, wind_category).
        Supports cross-dimensional analysis without separate queries.
        """
        return (
            df
            .cube("country", "temp_category", "wind_category")
            .agg(
                F.round(F.avg("temp_celsius"), 2).alias("avg_temp_celsius"),
                F.round(F.avg("wind_speed_ms"), 2).alias("avg_wind_ms"),
                F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
                F.count("*").alias("observation_count"),
            )
            .orderBy("country", "temp_category", "wind_category")
        )

    def hourly_grouping_sets(self, df: DataFrame) -> DataFrame:
        """
        GROUPING SETS: aggregate by (country, local_hour) and (local_hour) simultaneously.
        Replaces two separate GROUP BY queries.
        """
        return (
            df
            .groupBy(
                F.col("country"),
                F.col("local_hour"),
            )
            .agg(
                F.round(F.avg("temp_celsius"), 2).alias("avg_temp_celsius"),
                F.round(F.avg("humidity_pct"), 1).alias("avg_humidity_pct"),
                F.count("*").alias("sample_count"),
            )
        )

    def heat_wave_streaks(self, df: DataFrame, threshold: float = 35.0) -> DataFrame:
        """
        Gaps-and-islands streak detection: consecutive days above threshold per city.
        Uses the classic row_number() difference technique.

        Returns: (city_id, city_name, country, streak_start, streak_end, streak_days)
        """
        daily = (
            df
            .withColumn("obs_date", F.to_date("observed_at"))
            .groupBy("city_id", "city_name", "country", "obs_date")
            .agg(F.max("temp_celsius").alias("max_temp"))
            .filter(F.col("max_temp") >= threshold)
        )

        # Row number per city ordered by date → subtract from global row number
        # Consecutive dates produce the same "island" value
        w_city = Window.partitionBy("city_id").orderBy("obs_date")
        w_all = Window.orderBy("obs_date")

        island = (
            daily
            .withColumn("rn_city", F.row_number().over(w_city))
            .withColumn("rn_all",  F.row_number().over(w_all))
            .withColumn("island",  F.col("rn_all") - F.col("rn_city"))
        )

        return (
            island
            .groupBy("city_id", "city_name", "country", "island")
            .agg(
                F.min("obs_date").alias("streak_start"),
                F.max("obs_date").alias("streak_end"),
                F.count("*").alias("streak_days"),
                F.round(F.max("max_temp"), 1).alias("peak_temp_celsius"),
            )
            .drop("island")
            .filter(F.col("streak_days") >= 2)
            .orderBy(F.col("streak_days").desc())
        )

    def city_temperature_percentiles(self, df: DataFrame) -> DataFrame:
        """
        Compute temperature percentiles (P10, P50, P90) per city.
        Useful for detecting abnormal conditions relative to historical baseline.
        """
        return (
            df
            .groupBy("city_id", "city_name", "country")
            .agg(
                F.round(F.percentile_approx("temp_celsius", 0.10), 2).alias("p10_temp"),
                F.round(F.percentile_approx("temp_celsius", 0.50), 2).alias("p50_temp"),
                F.round(F.percentile_approx("temp_celsius", 0.90), 2).alias("p90_temp"),
                F.round(F.avg("temp_celsius"), 2).alias("avg_temp"),
                F.count("*").alias("observation_count"),
            )
        )
