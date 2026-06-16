"""
PostgreSQL connection + write helpers.
FIX v3: Fully implemented (was missing entirely in v2).
Uses SQLAlchemy 2.x with connection pooling.
No credentials in logs — DSN accessed via secrets manager only.
"""

import logging
from functools import lru_cache

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from src.security.secrets import get_settings

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Cached SQLAlchemy engine with connection pooling.
    Pool config is tuned for Supabase free tier (max 10 connections).
    """
    settings = get_settings()
    engine = create_engine(
        settings.get_db_url(),
        poolclass=QueuePool,
        pool_size=3,
        max_overflow=5,
        pool_timeout=30,
        pool_recycle=1800,          # Recycle connections after 30 min
        pool_pre_ping=True,         # Test connection health before use
        connect_args={
            "connect_timeout": 10,
            "application_name": "weather_dwh_pipeline",
            "options": "-c statement_timeout=30000",  # 30s query timeout
        },
    )
    logger.info("PostgreSQL engine created (pool_size=3)")
    return engine


def execute_sql(sql: str, params: dict = None) -> None:
    """Run a single SQL statement. Used for DDL and DML."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(sql), params or {})


def write_dataframe(df: pd.DataFrame, table: str, schema: str, if_exists: str = "append") -> None:
    """
    Write a pandas DataFrame to PostgreSQL.
    Uses SQLAlchemy engine — no credentials exposed to pandas.
    """
    engine = get_engine()
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
        method="multi",
        chunksize=500,
    )
    logger.info("Wrote %d rows to %s.%s", len(df), schema, table)


def write_spark_df_to_postgres(spark_df, table: str, schema: str, mode: str = "append") -> None:
    """
    Write a PySpark DataFrame to PostgreSQL via JDBC.
    Converts to pandas first (acceptable for the small sizes in this project).
    For large DataFrames, use spark_df.write.jdbc() with the JDBC driver instead.
    """
    pdf = spark_df.toPandas()
    write_dataframe(pdf, table=table, schema=schema, if_exists=mode)
    logger.info("Spark→PG: %d rows → %s.%s", len(pdf), schema, table)


def refresh_materialized_views() -> None:
    """
    Refresh all materialized views concurrently (no table lock).
    Called at the end of every pipeline run.
    """
    views = [
        "api.mv_country_snapshot",
        "api.mv_city_7day_trends",
    ]
    engine = get_engine()
    with engine.begin() as conn:
        for view in views:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
            logger.info("Refreshed: %s", view)


def upsert_observations(pdf: pd.DataFrame) -> int:
    """
    Upsert Silver observations into warehouse.fact_observations.
    Returns number of rows inserted/updated.
    """
    if pdf.empty:
        return 0

    engine = get_engine()
    rows_affected = 0
    with engine.begin() as conn:
        for _, row in pdf.iterrows():
            result = conn.execute(
                text("""
                    INSERT INTO warehouse.fact_observations (
                        city_sk, city_id, date_key, time_key,
                        observed_at, temp_celsius, feels_like_celsius,
                        pressure_hpa, humidity_pct, visibility_m,
                        wind_speed_ms, wind_deg, wind_gust_ms,
                        cloud_cover_pct, is_daytime, dq_score,
                        pipeline_run_id, year, month
                    )
                    VALUES (
                        :city_sk, :city_id, :date_key, :time_key,
                        :observed_at, :temp_celsius, :feels_like_celsius,
                        :pressure_hpa, :humidity_pct, :visibility_m,
                        :wind_speed_ms, :wind_deg, :wind_gust_ms,
                        :cloud_cover_pct, :is_daytime, :dq_score,
                        :pipeline_run_id, :year, :month
                    )
                    ON CONFLICT (city_id, observed_at) DO UPDATE SET
                        temp_celsius       = EXCLUDED.temp_celsius,
                        humidity_pct       = EXCLUDED.humidity_pct,
                        wind_speed_ms      = EXCLUDED.wind_speed_ms,
                        dq_score           = EXCLUDED.dq_score,
                        loaded_at          = NOW()
                """),
                row.to_dict(),
            )
            rows_affected += result.rowcount
    logger.info("Upserted %d observation rows into warehouse", rows_affected)
    return rows_affected
