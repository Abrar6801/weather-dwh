"""
Window function analytics for trend detection and anomaly scoring.
Operates on Silver-layer DataFrames.
"""

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def add_rolling_averages(df: DataFrame, days: int = 7) -> DataFrame:
    """
    Add rolling N-day averages for temperature, humidity, and wind per city.
    Uses a range window based on unix timestamp.
    """
    seconds = days * 86400
    w = (
        Window.partitionBy("city_id")
        .orderBy(F.unix_timestamp("observed_at"))
        .rangeBetween(-seconds, 0)
    )
    return (
        df
        .withColumn(f"rolling_{days}d_avg_temp", F.round(F.avg("temp_celsius").over(w), 2))
        .withColumn(f"rolling_{days}d_avg_humidity", F.round(F.avg("humidity_pct").over(w), 1))
        .withColumn(f"rolling_{days}d_avg_wind", F.round(F.avg("wind_speed_ms").over(w), 2))
    )


def add_lag_features(df: DataFrame) -> DataFrame:
    """
    Add 1-observation lag for temperature and wind (previous reading per city).
    Useful for computing delta/change metrics.
    """
    w = Window.partitionBy("city_id").orderBy("observed_at")
    return (
        df
        .withColumn("prev_temp_celsius", F.lag("temp_celsius", 1).over(w))
        .withColumn("temp_delta_celsius",
            F.round(F.col("temp_celsius") - F.lag("temp_celsius", 1).over(w), 2))
        .withColumn("prev_wind_speed_ms", F.lag("wind_speed_ms", 1).over(w))
    )


def rank_cities_by_metric(df: DataFrame, metric: str = "temp_celsius") -> DataFrame:
    """
    Rank cities by a given metric within each observation timestamp.
    Returns dense rank (no gaps) so cities with equal values share rank.
    """
    w = Window.partitionBy("observed_at").orderBy(F.col(metric).desc())
    return df.withColumn(f"rank_by_{metric}", F.dense_rank().over(w))


def detect_anomalies(df: DataFrame, z_threshold: float = 3.0) -> DataFrame:
    """
    Flag records where temperature deviates more than z_threshold standard deviations
    from the city's overall mean. Simple z-score anomaly detection.
    """
    stats = (
        df
        .groupBy("city_id")
        .agg(
            F.avg("temp_celsius").alias("city_mean_temp"),
            F.stddev("temp_celsius").alias("city_stddev_temp"),
        )
    )
    return (
        df
        .join(stats, on="city_id", how="left")
        .withColumn(
            "temp_z_score",
            F.round(
                (F.col("temp_celsius") - F.col("city_mean_temp"))
                / F.col("city_stddev_temp"),
                3,
            ),
        )
        .withColumn(
            "is_temp_anomaly",
            F.abs(F.col("temp_z_score")) > z_threshold,
        )
        .drop("city_mean_temp", "city_stddev_temp")
    )
