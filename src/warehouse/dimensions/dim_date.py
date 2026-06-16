"""
Date Dimension — Kimball-style full calendar table.
Populate once for a 10-year range; never changes.
FIX v3: fiscal_quarter formula tested against all 12 months, edge cases verified.
"""

import logging
from calendar import monthrange
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType, DateType, IntegerType, StringType, StructField, StructType
)

logger = logging.getLogger(__name__)

SCHEMA = StructType([
    StructField("date_key",       IntegerType(), False),  # YYYYMMDD
    StructField("full_date",      DateType(),    False),
    StructField("day_of_week",    IntegerType(), False),  # 1=Mon, 7=Sun
    StructField("day_name",       StringType(),  False),
    StructField("day_of_month",   IntegerType(), False),
    StructField("day_of_year",    IntegerType(), False),
    StructField("week_of_year",   IntegerType(), False),
    StructField("month_number",   IntegerType(), False),
    StructField("month_name",     StringType(),  False),
    StructField("month_short",    StringType(),  False),
    StructField("quarter",        IntegerType(), False),
    StructField("quarter_label",  StringType(),  False),
    StructField("year",           IntegerType(), False),
    StructField("year_month",     StringType(),  False),
    StructField("is_weekend",     BooleanType(), False),
    StructField("is_weekday",     BooleanType(), False),
    StructField("is_leap_year",   BooleanType(), False),
    StructField("days_in_month",  IntegerType(), False),
    StructField("fiscal_year",    IntegerType(), False),   # April-start
    StructField("fiscal_quarter", IntegerType(), False),
    StructField("season",         StringType(),  False),   # Northern hemisphere
])

_DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_MONTH_NAMES = [
    "", "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _fiscal(month: int, year: int) -> tuple[int, int]:
    """
    April-start fiscal year.
    Apr–Jun = Q1, Jul–Sep = Q2, Oct–Dec = Q3, Jan–Mar = Q4.
    FIX v3: All 12 months verified — no modulo edge cases.

    Month → (fiscal_year_offset, fiscal_quarter)
    Apr(4)=Q1, May(5)=Q1, Jun(6)=Q1
    Jul(7)=Q2, Aug(8)=Q2, Sep(9)=Q2
    Oct(10)=Q3, Nov(11)=Q3, Dec(12)=Q3
    Jan(1)=Q4, Feb(2)=Q4, Mar(3)=Q4
    """
    mapping = {
        4: (1, 1), 5: (1, 1), 6: (1, 1),
        7: (1, 2), 8: (1, 2), 9: (1, 2),
        10:(1, 3), 11:(1, 3), 12:(1, 3),
        1: (0, 4), 2: (0, 4), 3: (0, 4),
    }
    year_offset, quarter = mapping[month]
    return year + year_offset, quarter


def _is_leap(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def _season(month: int) -> str:
    if month in (12, 1, 2):
        return "Winter"
    if month in (3, 4, 5):
        return "Spring"
    if month in (6, 7, 8):
        return "Summer"
    return "Autumn"


class DimDateBuilder:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def generate(self, start: date, end: date) -> DataFrame:
        """Generate one row per calendar day from start to end inclusive."""
        rows = []
        current = start
        while current <= end:
            y, m, d = current.year, current.month, current.day
            dow = current.weekday() + 1
            quarter = (m - 1) // 3 + 1
            fy, fq = _fiscal(m, y)
            rows.append((
                int(current.strftime("%Y%m%d")),
                current,
                dow,
                _DAY_NAMES[dow - 1],
                d,
                current.timetuple().tm_yday,
                current.isocalendar()[1],
                m,
                _MONTH_NAMES[m],
                _MONTH_NAMES[m][:3],
                quarter,
                f"Q{quarter} {y}",
                y,
                current.strftime("%Y-%m"),
                dow >= 6,
                dow < 6,
                _is_leap(y),
                monthrange(y, m)[1],
                fy,
                fq,
                _season(m),
            ))
            current += timedelta(days=1)

        return self.spark.createDataFrame(rows, schema=SCHEMA)

    def write(self, df: DataFrame, path: str) -> None:
        df.write.format("delta").mode("overwrite").partitionBy("year").save(path)
        logger.info("dim_date written → %s (%d rows)", path, df.count())
