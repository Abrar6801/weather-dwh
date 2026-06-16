"""Tests for Bronze processor — schema casting and idempotency."""

import pytest
from datetime import datetime, timezone


SAMPLE_RECORDS = [
    {
        "city_id": 2643743,
        "city_name": "London",
        "country": "GB",
        "latitude": 51.5085,
        "longitude": -0.1257,
        "temp_celsius": 15.3,
        "feels_like_celsius": 14.1,
        "temp_min_celsius": 13.0,
        "temp_max_celsius": 17.5,
        "pressure_hpa": 1013,
        "humidity_pct": 72,
        "visibility_m": 10000,
        "wind_speed_ms": 4.1,
        "wind_deg": 270,
        "wind_gust_ms": 6.2,
        "cloud_cover_pct": 40,
        "weather_condition_id": 801,
        "weather_main": "Clouds",
        "weather_description": "few clouds",
        "weather_icon": "02d",
        "sunrise_utc": "2024-01-15T07:58:00+00:00",
        "sunset_utc": "2024-01-15T16:05:00+00:00",
        "observed_at_utc": "2024-01-15T12:00:00+00:00",
        "ingested_at_utc": "2024-01-15T12:01:00+00:00",
        "timezone_offset_sec": 0,
    }
]


def test_bronze_cast_schema(spark, tmp_path):
    """Bronze processor casts all columns to correct types."""
    import os
    with __import__("unittest.mock", fromlist=["patch"]).patch.dict(
        os.environ,
        {"BRONZE_DATA_PATH": str(tmp_path / "bronze"),
         "OWM_API_KEY": "k", "DATABASE_URL": "postgresql://u:p@h/d",
         "SUPABASE_URL": "https://x.supabase.co", "SUPABASE_ANON_KEY": "a",
         "JWT_SECRET_KEY": "a"*64, "API_KEY_HASH_SALT": "s",
         "FIELD_ENCRYPTION_KEY": "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3Q="},
    ):
        from src.security.secrets import get_settings
        get_settings.cache_clear()
        from src.processing.bronze import BronzeProcessor, BRONZE_SCHEMA
        proc = BronzeProcessor(spark=spark)
        raw_df = proc._records_to_df(SAMPLE_RECORDS)
        typed_df = proc._cast_schema(raw_df)
        # All schema columns present
        assert set(f.name for f in BRONZE_SCHEMA.fields).issubset(set(typed_df.columns))
        row = typed_df.first()
        assert row["city_id"] == 2643743
        assert abs(row["temp_celsius"] - 15.3) < 0.01


def test_bronze_idempotent(spark, tmp_path):
    """Running Bronze twice with same data produces same row count."""
    import os
    with __import__("unittest.mock", fromlist=["patch"]).patch.dict(
        os.environ,
        {"BRONZE_DATA_PATH": str(tmp_path / "bronze"),
         "OWM_API_KEY": "k", "DATABASE_URL": "postgresql://u:p@h/d",
         "SUPABASE_URL": "https://x.supabase.co", "SUPABASE_ANON_KEY": "a",
         "JWT_SECRET_KEY": "a"*64, "API_KEY_HASH_SALT": "s",
         "FIELD_ENCRYPTION_KEY": "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3Q="},
    ):
        from src.security.secrets import get_settings
        get_settings.cache_clear()
        from src.processing.bronze import BronzeProcessor
        proc = BronzeProcessor(spark=spark)
        proc.run(SAMPLE_RECORDS)
        proc.run(SAMPLE_RECORDS)   # Second run — MERGE, not append
        df = spark.read.format("delta").load(str(tmp_path / "bronze"))
        assert df.count() == 1     # Still 1 row, not 2
