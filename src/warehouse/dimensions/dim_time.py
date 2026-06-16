"""
Time Dimension — sub-daily time-of-day lookup.
FIX v3: Fully implemented (was missing in v2).
Generates one row per minute of the day (1,440 rows total).
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

logger = logging.getLogger(__name__)

SCHEMA = StructType([
    StructField("time_key",          IntegerType(), False),  # HHMM integer
    StructField("hour_24",           IntegerType(), False),
    StructField("minute",            IntegerType(), False),
    StructField("hour_12",           IntegerType(), False),
    StructField("am_pm",             StringType(),  False),
    StructField("time_label_24",     StringType(),  False),  # "14:30"
    StructField("time_label_12",     StringType(),  False),  # "2:30 PM"
    StructField("time_of_day",       StringType(),  False),  # "Afternoon"
    StructField("is_business_hour",  BooleanType(), False),  # 9–17
    StructField("is_peak_hour",      BooleanType(), False),  # 7–9, 17–19
])


def _time_of_day(hour: int) -> str:
    if 5 <= hour < 12:
        return "Morning"
    if 12 <= hour < 17:
        return "Afternoon"
    if 17 <= hour < 21:
        return "Evening"
    return "Night"


class DimTimeBuilder:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def build(self) -> DataFrame:
        """Generate all 1,440 minutes of the day."""
        rows = []
        for h in range(24):
            for m in range(60):
                h12 = h % 12 or 12
                am_pm = "AM" if h < 12 else "PM"
                rows.append((
                    h * 100 + m,
                    h,
                    m,
                    h12,
                    am_pm,
                    f"{h:02d}:{m:02d}",
                    f"{h12}:{m:02d} {am_pm}",
                    _time_of_day(h),
                    9 <= h < 17,
                    (7 <= h < 9) or (17 <= h < 19),
                ))
        return self.spark.createDataFrame(rows, schema=SCHEMA)

    def write(self, df: DataFrame, path: str) -> None:
        df.write.format("delta").mode("overwrite").save(path)
        logger.info("dim_time written → %s (%d rows)", path, df.count())
