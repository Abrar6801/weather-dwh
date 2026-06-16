"""
Analytics API router.
GET /rollup    — temperature ROLLUP by country/city/day
GET /trends    — 7-day city trends (temp delta, moving avg)
GET /extremes  — recent extreme weather events
"""

import logging

from fastapi import APIRouter, Depends, Query
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text

from src.api.auth import TokenData, get_current_user
from src.api.schemas import CountrySnapshotResponse, DailyTrendResponse, ExtremeEventResponse
from src.storage.postgres import get_engine

logger = logging.getLogger(__name__)
router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


@router.get("/rollup", response_model=list[CountrySnapshotResponse])
@limiter.limit("30/minute")
async def get_country_rollup(
    request=None,
    user: TokenData = Depends(get_current_user),
) -> list[CountrySnapshotResponse]:
    """
    Return the country-level snapshot from the materialized view.
    Shows current 24h averages per country — fast, pre-computed.
    """
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT country, city_count,
                       avg_temp_celsius, min_temp_celsius, max_temp_celsius,
                       avg_humidity_pct, avg_wind_ms, last_updated_utc
                FROM api.mv_country_snapshot
                ORDER BY avg_temp_celsius DESC NULLS LAST
            """)
        ).fetchall()

    return [
        CountrySnapshotResponse(
            country=r.country,
            city_count=r.city_count,
            avg_temp_celsius=float(r.avg_temp_celsius) if r.avg_temp_celsius is not None else None,
            min_temp_celsius=float(r.min_temp_celsius) if r.min_temp_celsius is not None else None,
            max_temp_celsius=float(r.max_temp_celsius) if r.max_temp_celsius is not None else None,
            avg_humidity_pct=float(r.avg_humidity_pct) if r.avg_humidity_pct is not None else None,
            avg_wind_ms=float(r.avg_wind_ms) if r.avg_wind_ms is not None else None,
            last_updated_utc=r.last_updated_utc,
        )
        for r in rows
    ]


@router.get("/trends", response_model=list[DailyTrendResponse])
@limiter.limit("30/minute")
async def get_7day_trends(
    request=None,
    city_id: int = Query(None, description="Filter by city ID"),
    user: TokenData = Depends(get_current_user),
) -> list[DailyTrendResponse]:
    """Return 7-day temperature trends from the materialized view."""
    engine = get_engine()
    with engine.connect() as conn:
        if city_id:
            rows = conn.execute(
                text("""
                    SELECT city_id, city_name, country, full_date AS obs_date,
                           avg_temp_celsius, min_temp_celsius, max_temp_celsius,
                           avg_humidity_pct, avg_wind_speed_ms, dominant_condition,
                           prev_day_temp_celsius, temp_delta_celsius
                    FROM api.mv_city_7day_trends
                    WHERE city_id = :city_id
                    ORDER BY full_date DESC
                """),
                {"city_id": city_id},
            ).fetchall()
        else:
            rows = conn.execute(
                text("""
                    SELECT city_id, city_name, country, full_date AS obs_date,
                           avg_temp_celsius, min_temp_celsius, max_temp_celsius,
                           avg_humidity_pct, avg_wind_speed_ms, dominant_condition,
                           prev_day_temp_celsius, temp_delta_celsius
                    FROM api.mv_city_7day_trends
                    ORDER BY full_date DESC, city_name
                    LIMIT 500
                """)
            ).fetchall()

    return [
        DailyTrendResponse(
            city_id=r.city_id,
            city_name=r.city_name,
            country=r.country,
            obs_date=r.obs_date,
            avg_temp_celsius=float(r.avg_temp_celsius) if r.avg_temp_celsius is not None else None,
            min_temp_celsius=float(r.min_temp_celsius) if r.min_temp_celsius is not None else None,
            max_temp_celsius=float(r.max_temp_celsius) if r.max_temp_celsius is not None else None,
            avg_humidity_pct=float(r.avg_humidity_pct) if r.avg_humidity_pct is not None else None,
            avg_wind_speed_ms=float(r.avg_wind_speed_ms) if r.avg_wind_speed_ms is not None else None,
            dominant_condition=r.dominant_condition,
            prev_day_temp_celsius=float(r.prev_day_temp_celsius) if r.prev_day_temp_celsius is not None else None,
            temp_delta_celsius=float(r.temp_delta_celsius) if r.temp_delta_celsius is not None else None,
        )
        for r in rows
    ]


@router.get("/extremes", response_model=list[ExtremeEventResponse])
@limiter.limit("30/minute")
async def get_extreme_events(
    request=None,
    limit: int = Query(50, ge=1, le=200, description="Max rows to return"),
    user: TokenData = Depends(get_current_user),
) -> list[ExtremeEventResponse]:
    """Return extreme weather events from the last 30 days."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT city_id, city_name, country, observed_at,
                       temp_celsius, wind_speed_ms, humidity_pct, visibility_m, event_type
                FROM warehouse.v_recent_extremes
                ORDER BY observed_at DESC
                LIMIT :limit
            """),
            {"limit": limit},
        ).fetchall()

    return [
        ExtremeEventResponse(
            city_id=r.city_id,
            city_name=r.city_name,
            country=r.country,
            observed_at=r.observed_at,
            temp_celsius=float(r.temp_celsius) if r.temp_celsius is not None else None,
            wind_speed_ms=float(r.wind_speed_ms) if r.wind_speed_ms is not None else None,
            humidity_pct=r.humidity_pct,
            visibility_m=r.visibility_m,
            event_type=r.event_type,
        )
        for r in rows
    ]
