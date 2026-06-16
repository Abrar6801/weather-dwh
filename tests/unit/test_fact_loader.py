"""Tests for fact loaders — surrogate key resolution and grain uniqueness."""

import datetime
import os
import pytest
from pyspark.sql import Row
from unittest.mock import patch


ENV = {
    "OWM_API_KEY": "k", "DATABASE_URL": "postgresql://u:p@h/d",
    "SUPABASE_URL": "https://x.supabase.co", "SUPABASE_ANON_KEY": "a",
    "JWT_SECRET_KEY": "a" * 64, "API_KEY_HASH_SALT": "s",
    "FIELD_ENCRYPTION_KEY": "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3Q=",
}


def _make_silver(spark):
    """Create minimal Silver-like DataFrame for fact loading tests."""
    from pyspark.sql.types import (
        BooleanType, DoubleType, IntegerType, LongType,
        StringType, StructField, StructType, TimestampType,
    )
    schema = StructType([
        StructField("city_id", LongType(), False),
        StructField("city_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timezone_offset_sec", IntegerType(), True),
        StructField("observed_at", TimestampType(), True),
        StructField("temp_celsius", DoubleType(), True),
        StructField("feels_like_celsius", DoubleType(), True),
        StructField("temp_min_celsius", DoubleType(), True),
        StructField("temp_max_celsius", DoubleType(), True),
        StructField("pressure_hpa", IntegerType(), True),
        StructField("humidity_pct", IntegerType(), True),
        StructField("visibility_m", IntegerType(), True),
        StructField("wind_speed_ms", DoubleType(), True),
        StructField("wind_deg", IntegerType(), True),
        StructField("wind_gust_ms", DoubleType(), True),
        StructField("cloud_cover_pct", IntegerType(), True),
        StructField("is_daytime", BooleanType(), True),
        StructField("dq_score", IntegerType(), True),
        StructField("heat_index_celsius", DoubleType(), True),
        StructField("weather_condition_id", IntegerType(), True),
        StructField("pipeline_run_id", StringType(), True),
        StructField("weather_main", StringType(), True),
        StructField("weather_description", StringType(), True),
        StructField("daylight_minutes", IntegerType(), True),
    ])
    rows = [(
        1, "London", "GB", 51.5, -0.1, 0,
        datetime.datetime(2024, 1, 15, 12, 0, 0),
        15.0, 14.0, 13.0, 17.0, 1013, 72, 10000,
        4.1, 270, 6.2, 40, True, 90,
        None, 801, "test-run-001", "Clouds", "few clouds", 480,
    )]
    return spark.createDataFrame(rows, schema=schema)


def _make_dim_city(spark, path: str):
    """Write a minimal dim_city Delta table."""
    import uuid
    from pyspark.sql.types import (
        BooleanType, DoubleType, IntegerType, LongType,
        StringType, StructField, StructType, TimestampType,
    )
    schema = StructType([
        StructField("city_sk", StringType(), False),
        StructField("city_id", LongType(), False),
        StructField("city_name", StringType(), False),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timezone_offset", IntegerType(), True),
        StructField("effective_from", TimestampType(), False),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
        StructField("row_hash", StringType(), False),
    ])
    rows = [(
        str(uuid.uuid4()), 1, "London", "GB", 51.5, -0.1, 0,
        datetime.datetime(2024, 1, 1), None, True, "abc123",
    )]
    df = spark.createDataFrame(rows, schema=schema)
    df.write.format("delta").mode("overwrite").save(path)
    return df


def test_fact_observations_surrogate_key_resolved(spark, tmp_path):
    """fact_observations: city_sk must be resolved from dim_city, not NULL."""
    wh = str(tmp_path)
    env = {**ENV, "WAREHOUSE_DATA_PATH": wh, "SILVER_DATA_PATH": str(tmp_path / "silver")}

    with patch.dict(os.environ, env, clear=True):
        from src.security.secrets import get_settings
        get_settings.cache_clear()

        _make_dim_city(spark, f"{wh}/dim_city")

        from src.warehouse.facts.fact_observations import FactObservationsLoader
        loader = FactObservationsLoader(spark=spark)
        silver_df = _make_silver(spark)
        fact_df = loader.resolve_keys(silver_df)

        rows = fact_df.collect()
        assert len(rows) == 1
        row = rows[0]
        assert row["city_sk"] is not None, "city_sk must be resolved from dim_city"
        assert row["date_key"] == 20240115
        assert row["time_key"] == 1200


def test_fact_observations_grain_uniqueness(spark, tmp_path):
    """Duplicate (city_id, observed_at) rows collapse to 1 after MERGE."""
    wh = str(tmp_path)
    env = {**ENV, "WAREHOUSE_DATA_PATH": wh, "SILVER_DATA_PATH": str(tmp_path / "silver")}

    with patch.dict(os.environ, env, clear=True):
        from src.security.secrets import get_settings
        get_settings.cache_clear()

        _make_dim_city(spark, f"{wh}/dim_city")

        from src.warehouse.facts.fact_observations import FactObservationsLoader
        loader = FactObservationsLoader(spark=spark)
        silver_df = _make_silver(spark)

        loader.load(silver_df)
        loader.load(silver_df)  # Second load — MERGE, not append

        result = spark.read.format("delta").load(f"{wh}/fact_observations")
        assert result.count() == 1, "MERGE must deduplicate on (city_id, observed_at)"


def test_fact_daily_weather_grain(spark, tmp_path):
    """fact_daily_weather: one row per (city_id, date_key)."""
    wh = str(tmp_path)
    env = {**ENV, "WAREHOUSE_DATA_PATH": wh, "SILVER_DATA_PATH": str(tmp_path / "silver")}

    with patch.dict(os.environ, env, clear=True):
        from src.security.secrets import get_settings
        get_settings.cache_clear()

        _make_dim_city(spark, f"{wh}/dim_city")

        from src.warehouse.facts.fact_daily_weather import FactDailyWeatherLoader
        loader = FactDailyWeatherLoader(spark=spark)
        silver_df = _make_silver(spark)
        fact_df = loader.build(silver_df)

        rows = fact_df.collect()
        assert len(rows) == 1
        assert rows[0]["date_key"] == 20240115
        assert rows[0]["city_id"] == 1
