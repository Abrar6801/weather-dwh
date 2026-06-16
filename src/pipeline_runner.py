"""
Full pipeline orchestrator.
Order: Ingest → Bronze → Silver → Gold → Dimensions (SCD2) → Facts → Sync PG → Refresh views
"""

import logging
from datetime import date

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def _clean(row_dict: dict) -> dict:
    """Convert pandas NaN/numpy scalars to Python-native types for SQLAlchemy."""
    result = {}
    for k, v in row_dict.items():
        try:
            if pd.isna(v):
                result[k] = None
                continue
        except (TypeError, ValueError):
            pass
        result[k] = v.item() if hasattr(v, "item") else v
    return result


def _sync_to_postgres(spark, settings, fact_obs_df, fact_daily_df) -> None:
    """Sync Delta Lake dims and facts to the PostgreSQL star schema."""
    from src.storage.postgres import get_engine
    engine = get_engine()
    wh = settings.warehouse_data_path

    # ── Static dims: populate once ────────────────────────────────────────────
    for delta_name, pg_table in [
        ("dim_date", "dim_date"),
        ("dim_time", "dim_time"),
        ("dim_weather_condition", "dim_weather_condition"),
    ]:
        with engine.connect() as conn:
            n = conn.execute(text(f"SELECT COUNT(*) FROM warehouse.{pg_table}")).scalar()
        if n > 0:
            logger.info("warehouse.%s already populated (%d rows) — skipping", pg_table, n)
            continue
        pdf = spark.read.format("delta").load(f"{wh}/{delta_name}").toPandas()
        pdf.to_sql(pg_table, engine, schema="warehouse", if_exists="append",
                   index=False, method="multi", chunksize=500)
        logger.info("Loaded %d rows → warehouse.%s", len(pdf), pg_table)

    # ── dim_city: upsert by city_sk ───────────────────────────────────────────
    pdf_city = spark.read.format("delta").load(f"{wh}/dim_city").toPandas()
    with engine.begin() as conn:
        for _, row in pdf_city.iterrows():
            conn.execute(text("""
                INSERT INTO warehouse.dim_city
                    (city_sk, city_id, city_name, country, latitude, longitude,
                     timezone_offset, effective_from, effective_to, is_current, row_hash)
                VALUES
                    (:city_sk, :city_id, :city_name, :country, :latitude, :longitude,
                     :timezone_offset, :effective_from, :effective_to, :is_current, :row_hash)
                ON CONFLICT (city_sk) DO UPDATE SET
                    effective_to = EXCLUDED.effective_to,
                    is_current   = EXCLUDED.is_current
            """), _clean(row.to_dict()))
    logger.info("Synced %d rows → warehouse.dim_city", len(pdf_city))

    # ── fact_observations: upsert by (city_id, observed_at) ──────────────────
    obs_cols = [
        "city_sk", "city_id", "date_key", "time_key", "condition_id",
        "observed_at", "pipeline_run_id", "temp_celsius", "feels_like_celsius",
        "temp_min_celsius", "temp_max_celsius", "heat_index_celsius",
        "pressure_hpa", "humidity_pct", "visibility_m", "wind_speed_ms",
        "wind_deg", "wind_gust_ms", "cloud_cover_pct", "is_daytime", "dq_score",
    ]
    available = set(fact_obs_df.columns)
    pdf_obs = fact_obs_df.select([c for c in obs_cols if c in available]).toPandas()
    with engine.begin() as conn:
        for _, row in pdf_obs.iterrows():
            conn.execute(text("""
                INSERT INTO warehouse.fact_observations
                    (city_sk, city_id, date_key, time_key, condition_id, observed_at,
                     pipeline_run_id, temp_celsius, feels_like_celsius, pressure_hpa,
                     humidity_pct, visibility_m, wind_speed_ms, wind_deg, wind_gust_ms,
                     cloud_cover_pct, is_daytime, dq_score)
                VALUES
                    (:city_sk, :city_id, :date_key, :time_key, :condition_id, :observed_at,
                     :pipeline_run_id, :temp_celsius, :feels_like_celsius, :pressure_hpa,
                     :humidity_pct, :visibility_m, :wind_speed_ms, :wind_deg, :wind_gust_ms,
                     :cloud_cover_pct, :is_daytime, :dq_score)
                ON CONFLICT (city_id, observed_at) DO UPDATE SET
                    temp_celsius  = EXCLUDED.temp_celsius,
                    humidity_pct  = EXCLUDED.humidity_pct,
                    wind_speed_ms = EXCLUDED.wind_speed_ms,
                    dq_score      = EXCLUDED.dq_score,
                    loaded_at     = NOW()
            """), _clean(row.to_dict()))
    logger.info("Synced %d rows → warehouse.fact_observations", len(pdf_obs))

    # ── fact_daily_weather: upsert by (city_id, date_key) ────────────────────
    pdf_daily = fact_daily_df.toPandas().rename(columns={
        "avg_feels_like_celsius": "avg_feels_like",
        "avg_cloud_cover_pct":    "avg_cloud_pct",
    })
    daily_cols = [
        "city_sk", "city_id", "city_name", "country", "date_key",
        "avg_temp_celsius", "min_temp_celsius", "max_temp_celsius",
        "stddev_temp", "avg_feels_like", "avg_humidity_pct", "max_humidity_pct",
        "avg_wind_speed_ms", "max_wind_speed_ms", "avg_pressure_hpa",
        "avg_visibility_m", "avg_cloud_pct", "dominant_condition",
        "daylight_minutes", "avg_dq_score", "observation_count",
    ]
    pdf_daily = pdf_daily[[c for c in daily_cols if c in pdf_daily.columns]]
    with engine.begin() as conn:
        for _, row in pdf_daily.iterrows():
            conn.execute(text("""
                INSERT INTO warehouse.fact_daily_weather
                    (city_sk, city_id, city_name, country, date_key,
                     avg_temp_celsius, min_temp_celsius, max_temp_celsius,
                     stddev_temp, avg_feels_like, avg_humidity_pct, max_humidity_pct,
                     avg_wind_speed_ms, max_wind_speed_ms, avg_pressure_hpa,
                     avg_visibility_m, avg_cloud_pct, dominant_condition,
                     daylight_minutes, avg_dq_score, observation_count)
                VALUES
                    (:city_sk, :city_id, :city_name, :country, :date_key,
                     :avg_temp_celsius, :min_temp_celsius, :max_temp_celsius,
                     :stddev_temp, :avg_feels_like, :avg_humidity_pct, :max_humidity_pct,
                     :avg_wind_speed_ms, :max_wind_speed_ms, :avg_pressure_hpa,
                     :avg_visibility_m, :avg_cloud_pct, :dominant_condition,
                     :daylight_minutes, :avg_dq_score, :observation_count)
                ON CONFLICT (city_id, date_key) DO UPDATE SET
                    avg_temp_celsius  = EXCLUDED.avg_temp_celsius,
                    avg_humidity_pct  = EXCLUDED.avg_humidity_pct,
                    observation_count = EXCLUDED.observation_count,
                    loaded_at         = NOW()
            """), _clean(row.to_dict()))
    logger.info("Synced %d rows → warehouse.fact_daily_weather", len(pdf_daily))


def run() -> None:
    from src.ingestion.owm_client import OpenWeatherMapClient
    from src.processing.bronze import BronzeProcessor
    from src.processing.gold import GoldProcessor
    from src.processing.silver import SilverProcessor
    from src.processing.spark_session import get_spark
    from src.security.secrets import get_settings
    from src.storage.postgres import refresh_materialized_views
    from src.warehouse.dimensions.dim_city import DimCityProcessor
    from src.warehouse.dimensions.dim_date import DimDateBuilder
    from src.warehouse.dimensions.dim_time import DimTimeBuilder
    from src.warehouse.dimensions.dim_weather import DimWeatherConditionBuilder
    from src.warehouse.facts.fact_daily_weather import FactDailyWeatherLoader
    from src.warehouse.facts.fact_observations import FactObservationsLoader

    settings = get_settings()
    spark = get_spark()

    logger.info("══ PIPELINE START ══")

    # ── Step 1: Ingest ────────────────────────────────────────────────────────
    logger.info("── 1/8 Ingestion")
    client = OpenWeatherMapClient()
    records = client.ingest_all(settings.cities_list)
    if not records:
        logger.error("No records ingested — aborting pipeline")
        spark.stop()
        return

    # ── Step 2: Bronze (Delta MERGE) ──────────────────────────────────────────
    logger.info("── 2/8 Bronze")
    bronze_df = BronzeProcessor(spark).run(records)

    # ── Step 3: Silver (CDC + DQ + Delta MERGE) ───────────────────────────────
    logger.info("── 3/8 Silver")
    silver_df = SilverProcessor(spark).run(input_df=bronze_df)

    # ── Step 4: Gold ──────────────────────────────────────────────────────────
    logger.info("── 4/8 Gold")
    GoldProcessor(spark).run(input_df=silver_df)

    # ── Step 5: Bootstrap static dimensions (idempotent) ─────────────────────
    logger.info("── 5/8 Dimensions")
    wh = settings.warehouse_data_path

    dim_date_path = f"{wh}/dim_date"
    try:
        spark.read.format("delta").load(dim_date_path)
        logger.info("dim_date already exists — skipping")
    except Exception:
        builder = DimDateBuilder(spark)
        df = builder.generate(date(2020, 1, 1), date(2030, 12, 31))
        builder.write(df, dim_date_path)

    dim_time_path = f"{wh}/dim_time"
    try:
        spark.read.format("delta").load(dim_time_path)
        logger.info("dim_time already exists — skipping")
    except Exception:
        b = DimTimeBuilder(spark)
        b.write(b.build(), dim_time_path)

    dim_weather_path = f"{wh}/dim_weather_condition"
    try:
        spark.read.format("delta").load(dim_weather_path)
        logger.info("dim_weather_condition already exists — skipping")
    except Exception:
        b = DimWeatherConditionBuilder(spark)
        b.write(b.build(), dim_weather_path)

    # ── Step 6: SCD2 city dimension ───────────────────────────────────────────
    logger.info("── 6/8 SCD2 dim_city")
    city_source = silver_df.select(
        "city_id", "city_name", "country", "latitude",
        "longitude", "timezone_offset_sec",
    ).distinct()
    DimCityProcessor(spark).apply_scd2(city_source)

    # ── Step 7: Facts → Delta Lake ────────────────────────────────────────────
    logger.info("── 7/8 Facts")
    fact_obs_df   = FactObservationsLoader(spark).load(silver_df)
    fact_daily_df = FactDailyWeatherLoader(spark).load(silver_df)

    # ── Step 7b: Sync dims + facts → PostgreSQL star schema ──────────────────
    logger.info("── 7b/8 Sync to PostgreSQL")
    try:
        _sync_to_postgres(spark, settings, fact_obs_df, fact_daily_df)
    except Exception as e:
        logger.error("PostgreSQL sync failed: %s", e, exc_info=True)
        raise

    # ── Step 8: Refresh PostgreSQL materialized views ─────────────────────────
    logger.info("── 8/8 Refresh materialized views")
    try:
        refresh_materialized_views()
    except Exception as e:
        logger.warning("Materialized view refresh failed (non-fatal): %s", e)

    spark.stop()
    logger.info("══ PIPELINE COMPLETE ══")


if __name__ == "__main__":
    run()
