"""Tests for Silver processor — DQ split, heat index, enrichment."""

import pytest
from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType,
    StructField, StructType,
)


def _make_df(spark, records):
    if not records:
        return spark.createDataFrame([], StructType([]))
    keys = list(records[0].keys())

    def _infer_field(k, v):
        if isinstance(v, float):
            return StructField(k, DoubleType(), True)
        if isinstance(v, int):
            return StructField(k, IntegerType(), True)
        return StructField(k, StringType(), True)

    schema = StructType([_infer_field(k, next((r[k] for r in records if r[k] is not None), "")) for k in keys])
    return spark.createDataFrame([[r[k] for k in keys] for r in records], schema=schema)


def test_dq_split_routes_correctly(spark):
    """Valid records go to Silver, invalid go to quarantine."""
    from src.processing.silver import SilverProcessor

    good = {"city_id": 1, "city_name": "Tokyo", "temp_celsius": 25.0,
            "observed_at_utc": "2024-01-15T12:00:00+00:00",
            "humidity_pct": 60, "bronze_loaded_at": "2024-01-15T12:01:00+00:00"}
    bad  = {"city_id": 2, "city_name": None, "temp_celsius": None,
            "observed_at_utc": "2024-01-15T12:00:00+00:00",
            "humidity_pct": 60, "bronze_loaded_at": "2024-01-15T12:01:00+00:00"}

    df = _make_df(spark, [good, bad])

    class _MockWatermark:
        def get_last_watermark(self): return None
        def update_watermark(self): pass

    proc = SilverProcessor.__new__(SilverProcessor)
    proc.watermark = _MockWatermark()

    valid_df, quarantine_df = proc.split_valid_quarantine(df)
    assert valid_df.count() == 1
    assert quarantine_df.count() == 1
    q_row = quarantine_df.first()
    assert q_row["quarantine_reason"] in ("null_city_name", "null_temperature")


def test_heat_index_only_for_valid_conditions(spark):
    """Heat index computed only when temp >= 27°C and humidity >= 40%."""
    from pyspark.sql import functions as F
    from src.processing.silver import SilverProcessor

    records = [
        # Should get heat index
        {"city_id": 1, "city_name": "Dubai", "country": "AE", "temp_celsius": 38.0,
         "humidity_pct": 70, "wind_speed_ms": 3.0,
         "observed_at_utc": "2024-07-01T12:00:00+00:00",
         "sunrise_utc": "2024-07-01T05:00:00+00:00",
         "sunset_utc": "2024-07-01T19:00:00+00:00",
         "timezone_offset_sec": 14400, "wind_gust_ms": None,
         "visibility_m": 8000, "weather_description": "clear sky"},
        # Too cold — no heat index
        {"city_id": 2, "city_name": "Oslo", "country": "NO", "temp_celsius": 5.0,
         "humidity_pct": 80, "wind_speed_ms": 2.0,
         "observed_at_utc": "2024-01-01T12:00:00+00:00",
         "sunrise_utc": "2024-01-01T09:00:00+00:00",
         "sunset_utc": "2024-01-01T15:00:00+00:00",
         "timezone_offset_sec": 3600, "wind_gust_ms": None,
         "visibility_m": 10000, "weather_description": "overcast clouds"},
    ]
    df = _make_df(spark, records)
    df = df.withColumn("observed_at", F.to_timestamp("observed_at_utc"))
    df = df.withColumn("sunrise", F.to_timestamp("sunrise_utc"))
    df = df.withColumn("sunset", F.to_timestamp("sunset_utc"))

    proc = SilverProcessor.__new__(SilverProcessor)
    enriched = proc.enrich(df)

    rows = {r["city_id"]: r for r in enriched.collect()}
    assert rows[1]["heat_index_celsius"] is not None
    assert rows[2]["heat_index_celsius"] is None
