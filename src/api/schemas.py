"""Pydantic response models for all API endpoints."""

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class LatestObservationResponse(BaseModel):
    city_id: int
    city_name: str
    country: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    observed_at: datetime
    temp_celsius: Optional[float] = None
    feels_like_celsius: Optional[float] = None
    humidity_pct: Optional[int] = None
    wind_speed_ms: Optional[float] = None
    wind_deg: Optional[int] = None
    cloud_cover_pct: Optional[int] = None
    visibility_m: Optional[int] = None
    weather_main: Optional[str] = None
    weather_description: Optional[str] = None
    is_daytime: Optional[bool] = None
    dq_score: Optional[int] = None


class CityHistoryResponse(BaseModel):
    city_id: int
    city_name: str
    country: str
    observed_at: datetime
    temp_celsius: Optional[float] = None
    humidity_pct: Optional[int] = None
    wind_speed_ms: Optional[float] = None
    weather_main: Optional[str] = None
    dq_score: Optional[int] = None


class DailyTrendResponse(BaseModel):
    city_id: int
    city_name: str
    country: str
    obs_date: date
    avg_temp_celsius: Optional[float] = None
    min_temp_celsius: Optional[float] = None
    max_temp_celsius: Optional[float] = None
    avg_humidity_pct: Optional[float] = None
    avg_wind_speed_ms: Optional[float] = None
    dominant_condition: Optional[str] = None
    prev_day_temp_celsius: Optional[float] = None
    temp_delta_celsius: Optional[float] = None


class CountrySnapshotResponse(BaseModel):
    country: str
    city_count: int
    avg_temp_celsius: Optional[float] = None
    min_temp_celsius: Optional[float] = None
    max_temp_celsius: Optional[float] = None
    avg_humidity_pct: Optional[float] = None
    avg_wind_ms: Optional[float] = None
    last_updated_utc: Optional[datetime] = None


class ExtremeEventResponse(BaseModel):
    city_id: int
    city_name: str
    country: str
    observed_at: datetime
    temp_celsius: Optional[float] = None
    wind_speed_ms: Optional[float] = None
    humidity_pct: Optional[int] = None
    visibility_m: Optional[int] = None
    event_type: str


class RollupResponse(BaseModel):
    country: Optional[str] = None
    city_name: Optional[str] = None
    obs_date: Optional[date] = None
    avg_temp_celsius: Optional[float] = None
    min_temp_celsius: Optional[float] = None
    max_temp_celsius: Optional[float] = None
    avg_humidity_pct: Optional[float] = None
    row_count: Optional[int] = None
    level: Optional[str] = None


class HealthResponse(BaseModel):
    status: str = "ok"


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_seconds: int = Field(default=1800)
