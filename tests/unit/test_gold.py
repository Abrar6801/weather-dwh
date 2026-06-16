"""Tests for Gold processor aggregations."""

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F


def _silver_df(spark):
    import datetime
    from pyspark.sql.types import (
        DoubleType, IntegerType, LongType, StringType,
        StructField, StructType, TimestampType,
    )
    schema = StructType([
        StructField("city_id", LongType(), False),
        StructField("city_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("temp_celsius", DoubleType(), True),
        StructField("feels_like_celsius", DoubleType(), True),
        StructField("humidity_pct", IntegerType(), True),
        StructField("wind_speed_ms", DoubleType(), True),
        StructField("wind_gust_ms", DoubleType(), True),
        StructField("cloud_cover_pct", IntegerType(), True),
        StructField("pressure_hpa", IntegerType(), True),
        StructField("visibility_m", IntegerType(), True),
        StructField("weather_main", StringType(), True),
        StructField("daylight_minutes", IntegerType(), True),
        StructField("dq_score", IntegerType(), True),
        StructField("observed_at", TimestampType(), True),
        StructField("local_hour", IntegerType(), True),
    ])
    rows = [
        (1, "London", "GB", 15.0, 14.0, 70, 4.0, None, 50, 1013, 9000,
         "Clouds", 480, 90, datetime.datetime(2024, 1, 15, 12, 0, 0), 12),
        (1, "London", "GB", 17.0, 16.0, 65, 3.5, 5.0, 40, 1015, 10000,
         "Clear", 480, 95, datetime.datetime(2024, 1, 15, 14, 0, 0), 14),
    ]
    return spark.createDataFrame(rows, schema=schema)


def test_city_daily_summary_grain(spark, tmp_path):
    """City daily summary produces one row per city per day."""
    import os
    with __import__("unittest.mock", fromlist=["patch"]).patch.dict(
        os.environ,
        {"GOLD_DATA_PATH": str(tmp_path / "gold"), "SILVER_DATA_PATH": str(tmp_path / "silver"),
         "OWM_API_KEY": "k", "DATABASE_URL": "postgresql://u:p@h/d",
         "SUPABASE_URL": "https://x.supabase.co", "SUPABASE_ANON_KEY": "a",
         "JWT_SECRET_KEY": "a"*64, "API_KEY_HASH_SALT": "s",
         "FIELD_ENCRYPTION_KEY": "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3Q="},
    ):
        from src.security.secrets import get_settings
        get_settings.cache_clear()
        from src.processing.gold import GoldProcessor
        proc = GoldProcessor(spark=spark)
        result = proc.city_daily_summary(_silver_df(spark))
        assert result.count() == 1   # One row for London on 2024-01-15
        row = result.first()
        assert abs(row["avg_temp_celsius"] - 16.0) < 0.1
        assert abs(row["min_temp_celsius"] - 15.0) < 0.1
        assert abs(row["max_temp_celsius"] - 17.0) < 0.1


def test_extreme_events_threshold(spark, tmp_path):
    """Extreme events only includes records above thresholds."""
    import os
    import datetime
    with __import__("unittest.mock", fromlist=["patch"]).patch.dict(
        os.environ,
        {"GOLD_DATA_PATH": str(tmp_path / "gold"), "SILVER_DATA_PATH": str(tmp_path / "silver"),
         "OWM_API_KEY": "k", "DATABASE_URL": "postgresql://u:p@h/d",
         "SUPABASE_URL": "https://x.supabase.co", "SUPABASE_ANON_KEY": "a",
         "JWT_SECRET_KEY": "a"*64, "API_KEY_HASH_SALT": "s",
         "FIELD_ENCRYPTION_KEY": "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3Q="},
    ):
        from src.security.secrets import get_settings
        get_settings.cache_clear()
        from src.processing.gold import GoldProcessor
        from pyspark.sql.types import (
            DoubleType, IntegerType, LongType, StringType,
            StructField, StructType, TimestampType,
        )
        schema = StructType([
            StructField("city_id", LongType(), False),
            StructField("city_name", StringType(), True),
            StructField("country", StringType(), True),
            StructField("temp_celsius", DoubleType(), True),
            StructField("feels_like_celsius", DoubleType(), True),
            StructField("humidity_pct", IntegerType(), True),
            StructField("wind_speed_ms", DoubleType(), True),
            StructField("wind_gust_ms", DoubleType(), True),
            StructField("cloud_cover_pct", IntegerType(), True),
            StructField("pressure_hpa", IntegerType(), True),
            StructField("visibility_m", IntegerType(), True),
            StructField("weather_main", StringType(), True),
            StructField("daylight_minutes", IntegerType(), True),
            StructField("dq_score", IntegerType(), True),
            StructField("observed_at", TimestampType(), True),
            StructField("local_hour", IntegerType(), True),
            StructField("wind_category", StringType(), True),
            StructField("temp_category", StringType(), True),
            StructField("weather_description", StringType(), True),
        ])
        rows = [
            (3, "Phoenix", "US", 42.0, 44.0, 10, 5.0, None, 0, 1005, 15000,
             "Clear", 840, 100, datetime.datetime(2024, 7, 4, 15, 0, 0), 15,
             "light", "hot", "clear sky"),
            (1, "London", "GB", 15.0, 14.0, 70, 4.0, None, 50, 1013, 9000,
             "Clouds", 480, 90, datetime.datetime(2024, 1, 15, 12, 0, 0), 12,
             "light", "mild", "few clouds"),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        proc = GoldProcessor(spark=spark)
        extremes = proc.extreme_events(df)
        assert extremes.count() == 1
        assert extremes.first()["city_name"] == "Phoenix"
        assert extremes.first()["event_type"] == "extreme_heat"
