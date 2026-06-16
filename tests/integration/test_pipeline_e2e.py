"""
End-to-end pipeline integration test.
Requires a real PostgreSQL connection (DATABASE_URL env var) and network access.
Skipped automatically in CI if DATABASE_URL is not set to a real DB.
"""

import os
import pytest


@pytest.mark.integration
def test_pipeline_runs_without_error(tmp_path, monkeypatch):
    """
    Smoke test: run the full pipeline with mocked OWM responses.
    Verifies Bronze → Silver → Gold writes complete without exceptions.
    """
    pytest.importorskip("delta")

    monkeypatch.setenv("BRONZE_DATA_PATH", str(tmp_path / "bronze"))
    monkeypatch.setenv("SILVER_DATA_PATH", str(tmp_path / "silver"))
    monkeypatch.setenv("GOLD_DATA_PATH", str(tmp_path / "gold"))
    monkeypatch.setenv("WAREHOUSE_DATA_PATH", str(tmp_path / "warehouse"))
    monkeypatch.setenv("QUARANTINE_DATA_PATH", str(tmp_path / "quarantine"))

    from unittest.mock import MagicMock, patch

    # Minimal mock record (matches owm_client.to_flat_dict() output)
    mock_record = {
        "city_id": 2643743,
        "city_name": "London",
        "country": "GB",
        "latitude": 51.5,
        "longitude": -0.1,
        "temp_celsius": 15.0,
        "feels_like_celsius": 14.0,
        "temp_min_celsius": 13.0,
        "temp_max_celsius": 17.0,
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

    with patch("src.ingestion.owm_client.OpenWeatherMapClient.ingest_all",
               return_value=[mock_record]):
        with patch("src.cdc.watermark.WatermarkManager._ensure_table"):
            with patch("src.cdc.watermark.WatermarkManager.get_last_watermark",
                       return_value=None):
                with patch("src.cdc.watermark.WatermarkManager.update_watermark"):
                    from src.processing.spark_session import get_spark
                    from src.processing.bronze import BronzeProcessor
                    from src.processing.silver import SilverProcessor
                    from src.processing.gold import GoldProcessor

                    spark = get_spark("E2E_Test")
                    bronze_df = BronzeProcessor(spark).run([mock_record])
                    silver_df = SilverProcessor(spark).run(input_df=bronze_df)
                    gold_tables = GoldProcessor(spark).run(input_df=silver_df)
                    spark.stop()

                    assert silver_df.count() >= 1
                    assert "city_daily_summary" in gold_tables
