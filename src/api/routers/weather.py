"""
Weather API router.
GET /latest         — latest observation per city (all cities)
GET /city/{id}      — latest observation for a specific city
GET /city/{id}/history — last 7 days of hourly observations for a city
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from slowapi import Limiter
from slowapi.util import get_remote_address
from sqlalchemy import text

from src.api.auth import TokenData, get_current_user
from src.api.schemas import CityHistoryResponse, LatestObservationResponse
from src.storage.postgres import get_engine

logger = logging.getLogger(__name__)
router = APIRouter()
limiter = Limiter(key_func=get_remote_address)


def _row_to_latest(row) -> LatestObservationResponse:
    return LatestObservationResponse(
        city_id=row.city_id,
        city_name=row.city_name,
        country=row.country,
        latitude=row.latitude,
        longitude=row.longitude,
        observed_at=row.observed_at,
        temp_celsius=float(row.temp_celsius) if row.temp_celsius is not None else None,
        feels_like_celsius=float(row.feels_like_celsius) if row.feels_like_celsius is not None else None,
        humidity_pct=row.humidity_pct,
        wind_speed_ms=float(row.wind_speed_ms) if row.wind_speed_ms is not None else None,
        wind_deg=row.wind_deg,
        cloud_cover_pct=row.cloud_cover_pct,
        visibility_m=row.visibility_m,
        is_daytime=row.is_daytime,
        dq_score=row.dq_score,
    )


@router.get("/latest", response_model=list[LatestObservationResponse])
@limiter.limit("60/minute")
async def get_latest_all(
    request=None,
    country: str | None = Query(None, description="Filter by 2-letter country code"),
    user: TokenData = Depends(get_current_user),
) -> list[LatestObservationResponse]:
    """Return the most recent observation for each tracked city."""
    engine = get_engine()
    with engine.connect() as conn:
        if country:
            rows = conn.execute(
                text("""
                    SELECT city_id, city_name, country, latitude, longitude,
                           observed_at, temp_celsius, feels_like_celsius,
                           humidity_pct, wind_speed_ms, wind_deg,
                           cloud_cover_pct, visibility_m, is_daytime, dq_score
                    FROM warehouse.v_latest_per_city
                    WHERE country = :country
                    ORDER BY city_name
                """),
                {"country": country.upper()},
            ).fetchall()
        else:
            rows = conn.execute(
                text("""
                    SELECT city_id, city_name, country, latitude, longitude,
                           observed_at, temp_celsius, feels_like_celsius,
                           humidity_pct, wind_speed_ms, wind_deg,
                           cloud_cover_pct, visibility_m, is_daytime, dq_score
                    FROM warehouse.v_latest_per_city
                    ORDER BY city_name
                """)
            ).fetchall()

    return [_row_to_latest(r) for r in rows]


@router.get("/city/{city_id}", response_model=LatestObservationResponse)
@limiter.limit("60/minute")
async def get_city_latest(
    city_id: int,
    request=None,
    user: TokenData = Depends(get_current_user),
) -> LatestObservationResponse:
    """Return the latest observation for a single city by OWM city ID."""
    engine = get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("""
                SELECT city_id, city_name, country, latitude, longitude,
                       observed_at, temp_celsius, feels_like_celsius,
                       humidity_pct, wind_speed_ms, wind_deg,
                       cloud_cover_pct, visibility_m, is_daytime, dq_score
                FROM warehouse.v_latest_per_city
                WHERE city_id = :city_id
            """),
            {"city_id": city_id},
        ).fetchone()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"City {city_id} not found",
        )
    return _row_to_latest(row)


@router.get("/city/{city_id}/history", response_model=list[CityHistoryResponse])
@limiter.limit("30/minute")
async def get_city_history(
    city_id: int,
    request=None,
    days: int = Query(7, ge=1, le=30, description="Number of days of history"),
    user: TokenData = Depends(get_current_user),
) -> list[CityHistoryResponse]:
    """Return hourly observations for a city over the last N days."""
    engine = get_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT o.city_id, c.city_name, c.country,
                       o.observed_at, o.temp_celsius, o.humidity_pct,
                       o.wind_speed_ms, o.dq_score
                FROM warehouse.fact_observations o
                JOIN warehouse.dim_city c ON o.city_sk = c.city_sk AND c.is_current
                WHERE o.city_id = :city_id
                  AND o.observed_at >= NOW() - (:days || ' days')::INTERVAL
                ORDER BY o.observed_at DESC
                LIMIT 720
            """),
            {"city_id": city_id, "days": days},
        ).fetchall()

    if not rows:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No history found for city {city_id}",
        )

    return [
        CityHistoryResponse(
            city_id=r.city_id,
            city_name=r.city_name,
            country=r.country,
            observed_at=r.observed_at,
            temp_celsius=float(r.temp_celsius) if r.temp_celsius is not None else None,
            humidity_pct=r.humidity_pct,
            wind_speed_ms=float(r.wind_speed_ms) if r.wind_speed_ms is not None else None,
            dq_score=r.dq_score,
        )
        for r in rows
    ]
