"""Tests for SCD Type 2 — change detection and history preservation."""

import pytest
from datetime import date


def _city_records(spark, records):
    from pyspark.sql import Row
    return spark.createDataFrame([Row(**r) for r in records])


BASE_CITIES = [
    {"city_id": 1, "city_name": "London", "country": "GB",
     "latitude": 51.5, "longitude": -0.1, "timezone_offset_sec": 0},
    {"city_id": 2, "city_name": "Paris", "country": "FR",
     "latitude": 48.8, "longitude": 2.3, "timezone_offset_sec": 3600},
]

UPDATED_CITIES = [
    # London unchanged
    {"city_id": 1, "city_name": "London", "country": "GB",
     "latitude": 51.5, "longitude": -0.1, "timezone_offset_sec": 0},
    # Paris — timezone changed (triggers SCD2 new row)
    {"city_id": 2, "city_name": "Paris", "country": "FR",
     "latitude": 48.8, "longitude": 2.3, "timezone_offset_sec": 7200},
]


def test_scd2_initial_load(spark, tmp_path):
    """Initial load creates one current row per city."""
    import os
    with __import__("unittest.mock", fromlist=["patch"]).patch.dict(
        os.environ,
        {"WAREHOUSE_DATA_PATH": str(tmp_path),
         "OWM_API_KEY": "k", "DATABASE_URL": "postgresql://u:p@h/d",
         "SUPABASE_URL": "https://x.supabase.co", "SUPABASE_ANON_KEY": "a",
         "JWT_SECRET_KEY": "a"*64, "API_KEY_HASH_SALT": "s",
         "FIELD_ENCRYPTION_KEY": "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3Q="},
    ):
        from src.security.secrets import get_settings
        get_settings.cache_clear()
        from src.warehouse.dimensions.dim_city import DimCityProcessor
        proc = DimCityProcessor(spark=spark)
        src_df = _city_records(spark, BASE_CITIES)
        proc.initialize(src_df)
        dim = spark.read.format("delta").load(proc.dim_path)
        assert dim.count() == 2
        assert dim.filter("is_current = true").count() == 2


def test_scd2_change_creates_history(spark, tmp_path):
    """Changing an attribute expires old row and inserts new current row."""
    import os
    with __import__("unittest.mock", fromlist=["patch"]).patch.dict(
        os.environ,
        {"WAREHOUSE_DATA_PATH": str(tmp_path),
         "OWM_API_KEY": "k", "DATABASE_URL": "postgresql://u:p@h/d",
         "SUPABASE_URL": "https://x.supabase.co", "SUPABASE_ANON_KEY": "a",
         "JWT_SECRET_KEY": "a"*64, "API_KEY_HASH_SALT": "s",
         "FIELD_ENCRYPTION_KEY": "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3Q="},
    ):
        from src.security.secrets import get_settings
        get_settings.cache_clear()
        from src.warehouse.dimensions.dim_city import DimCityProcessor
        proc = DimCityProcessor(spark=spark)

        proc.initialize(_city_records(spark, BASE_CITIES))
        proc.apply_scd2(_city_records(spark, UPDATED_CITIES))

        dim = spark.read.format("delta").load(proc.dim_path)
        # London: 1 row (unchanged)
        london = dim.filter("city_id = 1")
        assert london.count() == 1
        assert london.first()["is_current"] is True

        # Paris: 2 rows — old expired, new current
        paris = dim.filter("city_id = 2")
        assert paris.count() == 2
        assert paris.filter("is_current = true").count() == 1
        assert paris.filter("is_current = false").count() == 1
        expired = paris.filter("is_current = false").first()
        assert expired["effective_to"] is not None


def test_scd2_no_change_is_idempotent(spark, tmp_path):
    """Running SCD2 twice with same data produces no new rows."""
    import os
    with __import__("unittest.mock", fromlist=["patch"]).patch.dict(
        os.environ,
        {"WAREHOUSE_DATA_PATH": str(tmp_path),
         "OWM_API_KEY": "k", "DATABASE_URL": "postgresql://u:p@h/d",
         "SUPABASE_URL": "https://x.supabase.co", "SUPABASE_ANON_KEY": "a",
         "JWT_SECRET_KEY": "a"*64, "API_KEY_HASH_SALT": "s",
         "FIELD_ENCRYPTION_KEY": "dGVzdGtleXRlc3RrZXl0ZXN0a2V5dGVzdGtleXRlc3Q="},
    ):
        from src.security.secrets import get_settings
        get_settings.cache_clear()
        from src.warehouse.dimensions.dim_city import DimCityProcessor
        proc = DimCityProcessor(spark=spark)
        src = _city_records(spark, BASE_CITIES)
        proc.initialize(src)
        proc.apply_scd2(src)   # Same data — no changes
        dim = spark.read.format("delta").load(proc.dim_path)
        assert dim.count() == 2   # Still exactly 2 rows


def test_dim_date_fiscal_quarters():
    """Verify April-start fiscal year quarter mapping for all 12 months."""
    from src.warehouse.dimensions.dim_date import _fiscal
    # Apr-Jun → Q1 of next fiscal year
    assert _fiscal(4, 2024) == (2025, 1)
    assert _fiscal(5, 2024) == (2025, 1)
    assert _fiscal(6, 2024) == (2025, 1)
    # Jul-Sep → Q2
    assert _fiscal(7, 2024) == (2025, 2)
    assert _fiscal(9, 2024) == (2025, 2)
    # Oct-Dec → Q3
    assert _fiscal(10, 2024) == (2025, 3)
    assert _fiscal(12, 2024) == (2025, 3)
    # Jan-Mar → Q4 of current fiscal year
    assert _fiscal(1, 2025) == (2025, 4)
    assert _fiscal(3, 2025) == (2025, 4)
