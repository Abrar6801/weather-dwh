"""Delta Lake helper utilities for reading and writing Delta tables."""

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """Read a Delta table from path."""
    return spark.read.format("delta").load(path)


def write_delta(df: DataFrame, path: str, mode: str = "overwrite", partition_by: list[str] = None) -> None:
    """Write a DataFrame as a Delta table."""
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
    logger.info("Delta write → %s (mode=%s, rows=%d)", path, mode, df.count())


def merge_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_condition: str,
) -> None:
    """
    Generic Delta MERGE — update matched rows, insert new rows.
    Creates the table on first run.
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        tbl = DeltaTable.forPath(spark, target_path)
        (
            tbl.alias("target")
            .merge(source_df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info("Delta MERGE complete → %s", target_path)
    else:
        source_df.write.format("delta").mode("overwrite").save(target_path)
        logger.info("Delta table created → %s", target_path)


def table_exists(spark: SparkSession, path: str) -> bool:
    """Return True if a Delta table exists at path."""
    return DeltaTable.isDeltaTable(spark, path)
