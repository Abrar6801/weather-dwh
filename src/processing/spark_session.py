"""
SparkSession factory. Single entry point — never build SparkSession elsewhere.
"""

import logging
import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.security.secrets import get_settings

# Force Spark workers to use the same Python as the driver (avoids version mismatch on WSL)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

logger = logging.getLogger(__name__)


def get_spark(app_name: str = None) -> SparkSession:
    """
    Return a configured SparkSession.
    - Delta Lake extensions enabled (ACID, MERGE, time travel)
    - Adaptive Query Execution on
    - Credential logging suppressed
    - Tuned for 2GB RAM (free-tier machines and GitHub Actions)
    """
    settings = get_settings()
    name = app_name or settings.spark_app_name

    builder = (
        SparkSession.builder
        .master(settings.spark_master)
        .appName(name)
        # ── Delta Lake ───────────────────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Memory (free-tier safe) ──────────────────────────────────────────
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        # ── Adaptive Query Execution ─────────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # ── Parquet ──────────────────────────────────────────────────────────
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.parquet.mergeSchema", "false")
        .config("spark.sql.parquet.filterPushdown", "true")
        # ── Python version consistency (driver and workers must match) ──────
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        # ── Security: suppress logs that may contain params ──────────────────
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.eventLog.enabled", "false")
        .config("spark.hadoop.fs.permissions.umask-mode", "077")
        # ── Delta optimizations ──────────────────────────────────────────────
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready | master=%s | app=%s", settings.spark_master, name)
    return spark
