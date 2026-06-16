"""
Secrets Manager — vault pattern.
Single gateway for all credentials. Never use os.environ directly in business logic.
"""

import logging
import re
from functools import lru_cache

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """
    Pydantic Settings v2. SecretStr fields are never logged or repr'd as plaintext.
    Reads from .env file or environment variables (env vars take precedence).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── API Keys ────────────────────────────────────────────────────────────────
    owm_api_key: SecretStr

    # ── Database ────────────────────────────────────────────────────────────────
    database_url: SecretStr
    supabase_url: str
    supabase_anon_key: SecretStr

    # ── Auth ────────────────────────────────────────────────────────────────────
    jwt_secret_key: SecretStr
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 30
    api_key_hash_salt: SecretStr

    # ── Encryption ──────────────────────────────────────────────────────────────
    field_encryption_key: SecretStr

    # ── Spark ───────────────────────────────────────────────────────────────────
    spark_master: str = "local[*]"
    spark_app_name: str = "WeatherDWH"

    # ── Paths ───────────────────────────────────────────────────────────────────
    raw_data_path: str = "data/raw"
    bronze_data_path: str = "data/bronze"
    silver_data_path: str = "data/silver"
    gold_data_path: str = "data/gold"
    warehouse_data_path: str = "data/warehouse"
    quarantine_data_path: str = "data/quarantine"

    # ── Pipeline ────────────────────────────────────────────────────────────────
    cities: str = "London,New York,Tokyo,Sydney,Dubai,Los Angeles,Berlin,Mumbai"
    ingest_cron: str = "0 * * * *"

    # ── API Security ────────────────────────────────────────────────────────────
    allowed_origins: str = "http://localhost:3000,http://localhost:8501"
    max_api_requests_per_minute: int = 60

    # ── Properties ──────────────────────────────────────────────────────────────

    @property
    def cities_list(self) -> list[str]:
        return [c.strip() for c in self.cities.split(",") if c.strip()]

    @property
    def allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",") if o.strip()]

    # ── Secret accessors (call only at the point of use) ────────────────────────

    def get_db_url(self) -> str:
        """Return DB URL. Log-safe version available via mask_dsn()."""
        return self.database_url.get_secret_value()

    def get_owm_key(self) -> str:
        return self.owm_api_key.get_secret_value()

    def get_jwt_secret(self) -> str:
        return self.jwt_secret_key.get_secret_value()

    def get_encryption_key(self) -> bytes:
        """Return Fernet key as bytes."""
        return self.field_encryption_key.get_secret_value().encode()

    def get_api_salt(self) -> str:
        return self.api_key_hash_salt.get_secret_value()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached singleton — .env is read once per process."""
    s = Settings()
    logger.info("Settings loaded | DB: %s", _mask_dsn(s.get_db_url()))
    return s


def _mask_dsn(dsn: str) -> str:
    """Replace password in DSN with *** for safe logging."""
    return re.sub(r":([^@/]+)@", ":***@", dsn)
