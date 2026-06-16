"""
CDC Watermark Manager — PostgreSQL-backed.
FIX v3: Was file-based (.watermarks/ dir), which is ephemeral on GitHub Actions.
Now stored in a pipeline_watermarks table in PostgreSQL — survives between runs.
"""

import logging
from datetime import UTC, datetime

from sqlalchemy import text

from src.storage.postgres import get_engine

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pipeline.watermarks (
    layer            VARCHAR(50)  PRIMARY KEY,
    last_processed_at TIMESTAMPTZ NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
"""


class WatermarkManager:
    """
    Reads and writes per-layer watermarks to PostgreSQL.
    Thread-safe (each call opens a short-lived connection from the pool).
    """

    def __init__(self, layer: str) -> None:
        self.layer = layer
        self._ensure_table()

    def _ensure_table(self) -> None:
        """Create watermarks table if it doesn't exist."""
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS pipeline"))
            conn.execute(text(CREATE_TABLE_SQL))

    def get_last_watermark(self) -> str | None:
        """Return ISO timestamp string of last processed record, or None."""
        engine = get_engine()
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT last_processed_at FROM pipeline.watermarks WHERE layer = :layer"
                ),
                {"layer": self.layer},
            ).fetchone()
        if row:
            return row[0].isoformat()
        return None

    def update_watermark(self, timestamp: str = None) -> None:
        """Upsert the watermark for this layer."""
        ts = timestamp or datetime.now(tz=UTC).isoformat()
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO pipeline.watermarks (layer, last_processed_at, updated_at)
                    VALUES (:layer, CAST(:ts AS timestamptz), NOW())
                    ON CONFLICT (layer) DO UPDATE
                    SET last_processed_at = EXCLUDED.last_processed_at,
                        updated_at = NOW()
                """),
                {"layer": self.layer, "ts": ts},
            )
        logger.info("Watermark updated [%s] → %s", self.layer, ts)

    def reset(self) -> None:
        """Delete watermark — forces full reload on next run."""
        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM pipeline.watermarks WHERE layer = :layer"),
                {"layer": self.layer},
            )
        logger.warning("Watermark reset for layer: %s", self.layer)
