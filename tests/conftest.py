"""Shared pytest fixtures."""

import os
import sys
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Ensure PySpark workers use the same Python as the driver (venv Python 3.11)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """SparkSession for all unit tests — local mode, Delta enabled."""
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("WeatherDWH_Tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
