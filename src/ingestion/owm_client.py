"""
OpenWeatherMap API client.
Security: key accessed only via secrets manager, never logged, stripped from saved JSON.
"""

import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

import requests
from pydantic import BaseModel, Field, field_validator

from src.security.secrets import get_settings

logger = logging.getLogger(__name__)


class WeatherMain(BaseModel):
    temp: float
    feels_like: float
    temp_min: float
    temp_max: float
    pressure: int
    humidity: int


class WeatherCondition(BaseModel):
    id: int
    main: str
    description: str
    icon: str


class WindData(BaseModel):
    speed: float
    deg: int
    gust: float | None = None


class SysData(BaseModel):
    country: str
    sunrise: int
    sunset: int


class CurrentWeatherResponse(BaseModel):
    city_id: int = Field(alias="id")
    city_name: str = Field(alias="name")
    coord: dict
    weather: list[WeatherCondition]
    main: WeatherMain
    visibility: int | None = None
    wind: WindData
    clouds: dict
    dt: int
    sys: SysData
    timezone: int
    cod: int

    model_config = {"populate_by_name": True}

    @field_validator("weather")
    @classmethod
    def at_least_one_condition(cls, v: list) -> list:
        if not v:
            raise ValueError("weather list must not be empty")
        return v

    def to_flat_dict(self) -> dict:
        """Flatten to a single dict suitable for PySpark createDataFrame."""
        return {
            "city_id": self.city_id,
            "city_name": self.city_name,
            "country": self.sys.country,
            "latitude": self.coord.get("lat"),
            "longitude": self.coord.get("lon"),
            "temp_celsius": round(self.main.temp - 273.15, 2),
            "feels_like_celsius": round(self.main.feels_like - 273.15, 2),
            "temp_min_celsius": round(self.main.temp_min - 273.15, 2),
            "temp_max_celsius": round(self.main.temp_max - 273.15, 2),
            "pressure_hpa": self.main.pressure,
            "humidity_pct": self.main.humidity,
            "visibility_m": self.visibility,
            "wind_speed_ms": self.wind.speed,
            "wind_deg": self.wind.deg,
            "wind_gust_ms": self.wind.gust,
            "cloud_cover_pct": self.clouds.get("all"),
            "weather_condition_id": self.weather[0].id,
            "weather_main": self.weather[0].main,
            "weather_description": self.weather[0].description,
            "weather_icon": self.weather[0].icon,
            "sunrise_utc": datetime.fromtimestamp(
                self.sys.sunrise, tz=UTC
            ).isoformat(),
            "sunset_utc": datetime.fromtimestamp(
                self.sys.sunset, tz=UTC
            ).isoformat(),
            "observed_at_utc": datetime.fromtimestamp(
                self.dt, tz=UTC
            ).isoformat(),
            "ingested_at_utc": datetime.now(tz=UTC).isoformat(),
            "timezone_offset_sec": self.timezone,
        }


class OpenWeatherMapClient:
    """
    Thread-safe OWM API client.
    Retry with exponential backoff. API key never stored as plain attribute.
    """

    BASE_URL = "https://api.openweathermap.org/data/2.5"
    MAX_RETRIES = 3
    _SENSITIVE_PARAMS = {"appid", "api_key", "key"}

    def __init__(self) -> None:
        settings = get_settings()
        self._session = requests.Session()
        # SecretStr.get_secret_value() called once here at construction
        self._session.params = {"appid": settings.get_owm_key()}  # type: ignore[assignment]
        self.raw_path = Path(settings.raw_data_path)
        self.raw_path.mkdir(parents=True, exist_ok=True)

    def _get(self, endpoint: str, params: dict) -> dict:
        """GET with retry and exponential backoff. Never logs the full URL."""
        url = f"{self.BASE_URL}/{endpoint}"
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, params=params, timeout=10)
                if resp.status_code == 429:
                    wait = 2**attempt
                    logger.warning("Rate limited — waiting %ds (attempt %d)", wait, attempt)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.Timeout:
                logger.warning("Timeout on attempt %d for endpoint=%s", attempt, endpoint)
                if attempt == self.MAX_RETRIES:
                    raise
                time.sleep(2**attempt)
            except requests.HTTPError:
                logger.error("HTTP %s on endpoint=%s", resp.status_code, endpoint)
                raise
        raise RuntimeError(f"All {self.MAX_RETRIES} retries exhausted for {endpoint}")

    def _save_raw(self, data: dict, prefix: str) -> Path:
        """Persist JSON with sensitive params stripped."""
        now = datetime.now(tz=UTC)
        folder = self.raw_path / prefix / now.strftime("%Y/%m/%d")
        folder.mkdir(parents=True, exist_ok=True)
        filename = f"{prefix}_{now.strftime('%Y%m%d_%H%M%S_%f')}.json"
        path = folder / filename
        safe = {k: v for k, v in data.items() if k.lower() not in self._SENSITIVE_PARAMS}
        with path.open("w", encoding="utf-8") as f:
            json.dump(safe, f, ensure_ascii=False, indent=2)
        return path

    def get_current_weather(self, city: str) -> CurrentWeatherResponse:
        raw = self._get("weather", {"q": city, "units": "standard"})
        return CurrentWeatherResponse(**raw)

    def ingest_city(self, city: str) -> dict:
        logger.info("Ingesting: %s", city)
        current = self.get_current_weather(city)
        self._save_raw(current.model_dump(), prefix="current")
        flat = current.to_flat_dict()
        logger.info("✓ %s | %.1f°C | %s", city, flat["temp_celsius"], flat["weather_description"])
        return flat

    def ingest_all(self, cities: list[str], delay: float = 0.5) -> list[dict]:
        """Ingest a list of cities with rate-limit-safe delays. Skips failures."""
        results = []
        for city in cities:
            try:
                results.append(self.ingest_city(city))
            except Exception as exc:
                logger.error("Skipping %s: %s", city, exc)
            time.sleep(delay)
        logger.info("Ingestion complete: %d/%d succeeded", len(results), len(cities))
        return results
