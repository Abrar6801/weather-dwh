"""
Full pipeline orchestrator.
Order: Ingest → Bronze → Silver → Gold → Dimensions (SCD2) → Facts → Refresh views
"""

import logging
from datetime import date

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run() -> None:
    from src.security.secrets import get_settings
    from src.ingestion.owm_client import OpenWeatherMapClient
    from src.processing.bronze import BronzeProcessor
    from src.processing.silver import SilverProcessor
    from src.processing.gold import GoldProcessor
    from src.processing.spark_session import get_spark
    from src.warehouse.dimensions.dim_date import DimDateBuilder
    from src.warehouse.dimensions.dim_city import DimCityProcessor
    from src.warehouse.dimensions.dim_weather import DimWeatherConditionBuilder
    from src.warehouse.dimensions.dim_time import DimTimeBuilder
    from src.warehouse.facts.fact_observations import FactObservationsLoader
    from src.warehouse.facts.fact_daily_weather import FactDailyWeatherLoader
    from src.storage.postgres import refresh_materialized_views

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

    # ── Step 7: Facts ─────────────────────────────────────────────────────────
    logger.info("── 7/8 Facts")
    FactObservationsLoader(spark).load(silver_df)
    FactDailyWeatherLoader(spark).load(silver_df)

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
