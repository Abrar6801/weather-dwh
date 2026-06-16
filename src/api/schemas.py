"""Pydantic response models for all API endpoints."""

from datetime import date, datetime

from pydantic import BaseModel, Field


class LatestObservationResponse(BaseModel):
    city_id: int
    city_name: str
    country: str
    latitude: float | None = None
    longitude: float | None = None
    observed_at: datetime
    temp_celsius: float | None = None
    feels_like_celsius: float | None = None
    humidity_pct: int | None = None
    wind_speed_ms: float | None = None
    wind_deg: int | None = None
    cloud_cover_pct: int | None = None
    visibility_m: int | None = None
    weather_main: str | None = None
    weather_description: str | None = None
    is_daytime: bool | None = None
    dq_score: int | None = None


class CityHistoryResponse(BaseModel):
    city_id: int
    city_name: str
    country: str
    observed_at: datetime
    temp_celsius: float | None = None
    humidity_pct: int | None = None
    wind_speed_ms: float | None = None
    weather_main: str | None = None
    dq_score: int | None = None


class DailyTrendResponse(BaseModel):
    city_id: int
    city_name: str
    country: str
    obs_date: date
    avg_temp_celsius: float | None = None
    min_temp_celsius: float | None = None
    max_temp_celsius: float | None = None
    avg_humidity_pct: float | None = None
    avg_wind_speed_ms: float | None = None
    dominant_condition: str | None = None
    prev_day_temp_celsius: float | None = None
    temp_delta_celsius: float | None = None


class CountrySnapshotResponse(BaseModel):
    country: str
    city_count: int
    avg_temp_celsius: float | None = None
    min_temp_celsius: float | None = None
    max_temp_celsius: float | None = None
    avg_humidity_pct: float | None = None
    avg_wind_ms: float | None = None
    last_updated_utc: datetime | None = None


class ExtremeEventResponse(BaseModel):
    city_id: int
    city_name: str
    country: str
    observed_at: datetime
    temp_celsius: float | None = None
    wind_speed_ms: float | None = None
    humidity_pct: int | None = None
    visibility_m: int | None = None
    event_type: str


class RollupResponse(BaseModel):
    country: str | None = None
    city_name: str | None = None
    obs_date: date | None = None
    avg_temp_celsius: float | None = None
    min_temp_celsius: float | None = None
    max_temp_celsius: float | None = None
    avg_humidity_pct: float | None = None
    row_count: int | None = None
    level: str | None = None


class HealthResponse(BaseModel):
    status: str = "ok"


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in_seconds: int = Field(default=1800)
