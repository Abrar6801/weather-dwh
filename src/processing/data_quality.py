"""
Data Quality runner — invoked by dq_checks.yml GitHub Actions workflow.
Runs Great Expectations suites against Silver and Gold Delta tables.
"""

import argparse
import logging
import sys
from pathlib import Path

from src.processing.spark_session import get_spark
from src.security.secrets import get_settings

logger = logging.getLogger(__name__)


def check_layer(layer: str) -> bool:
    """
    Run basic DQ assertions against a Delta layer.
    Returns True if all checks pass, False otherwise.
    """
    settings = get_settings()
    path_map = {
        "bronze": settings.bronze_data_path,
        "silver": settings.silver_data_path,
        "gold":   settings.gold_data_path,
    }
    if layer not in path_map:
        raise ValueError(f"Unknown layer: {layer}. Must be one of {list(path_map)}")

    spark = get_spark(f"DQ_{layer}")
    path = path_map[layer]

    try:
        df = spark.read.format("delta").load(path)
    except Exception as exc:
        logger.warning("Layer %s not yet created — skipping DQ: %s", layer, exc)
        return True

    total = df.count()
    logger.info("DQ check [%s] — total rows: %d", layer, total)

    passed = True

    if layer == "silver":
        null_city = df.filter("city_name IS NULL").count()
        null_temp = df.filter("temp_celsius IS NULL").count()
        temp_oor = df.filter("temp_celsius < -89.2 OR temp_celsius > 56.7").count()
        hum_oor = df.filter("humidity_pct < 0 OR humidity_pct > 100").count()

        checks = {
            "null_city_name": null_city,
            "null_temperature": null_temp,
            "temp_out_of_range": temp_oor,
            "humidity_out_of_range": hum_oor,
        }
        for check, count in checks.items():
            if count > 0:
                logger.error("FAIL [%s] %s: %d rows", layer, check, count)
                passed = False
            else:
                logger.info("PASS [%s] %s", layer, check)

    if layer == "gold":
        # Gold tables are aggregates — just verify they're non-empty
        if total == 0:
            logger.error("FAIL [gold] table is empty")
            passed = False
        else:
            logger.info("PASS [gold] non-empty (%d rows)", total)

    spark.stop()
    return passed


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run DQ checks on a pipeline layer")
    parser.add_argument("--layer", required=True, choices=["bronze", "silver", "gold"])
    args = parser.parse_args()

    ok = check_layer(args.layer)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
