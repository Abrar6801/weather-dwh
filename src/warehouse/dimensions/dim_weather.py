"""
Weather Condition Dimension — static lookup table.
FIX v3: Fully implemented (was missing in v2).
Sourced from OWM condition codes: https://openweathermap.org/weather-conditions
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

logger = logging.getLogger(__name__)

SCHEMA = StructType([
    StructField("condition_id",  IntegerType(), False),  # OWM condition code
    StructField("main_category", StringType(),  False),
    StructField("description",   StringType(),  False),
    StructField("icon_day",      StringType(),  True),
    StructField("icon_night",    StringType(),  True),
    StructField("severity",      IntegerType(), False),  # 1=benign, 5=extreme
])

# Representative subset of OWM condition codes
_CONDITIONS = [
    (200, "Thunderstorm", "thunderstorm with light rain", "11d", "11n", 4),
    (201, "Thunderstorm", "thunderstorm with rain", "11d", "11n", 4),
    (202, "Thunderstorm", "thunderstorm with heavy rain", "11d", "11n", 5),
    (210, "Thunderstorm", "light thunderstorm", "11d", "11n", 3),
    (211, "Thunderstorm", "thunderstorm", "11d", "11n", 4),
    (212, "Thunderstorm", "heavy thunderstorm", "11d", "11n", 5),
    (300, "Drizzle",      "light intensity drizzle", "09d", "09n", 2),
    (301, "Drizzle",      "drizzle", "09d", "09n", 2),
    (302, "Drizzle",      "heavy intensity drizzle", "09d", "09n", 2),
    (500, "Rain",         "light rain", "10d", "10n", 2),
    (501, "Rain",         "moderate rain", "10d", "10n", 3),
    (502, "Rain",         "heavy intensity rain", "10d", "10n", 4),
    (511, "Rain",         "freezing rain", "13d", "13n", 4),
    (600, "Snow",         "light snow", "13d", "13n", 3),
    (601, "Snow",         "snow", "13d", "13n", 3),
    (602, "Snow",         "heavy snow", "13d", "13n", 4),
    (611, "Snow",         "sleet", "13d", "13n", 3),
    (701, "Mist",         "mist", "50d", "50n", 1),
    (711, "Smoke",        "smoke", "50d", "50n", 2),
    (721, "Haze",         "haze", "50d", "50n", 1),
    (731, "Dust",         "sand/dust whirls", "50d", "50n", 2),
    (741, "Fog",          "fog", "50d", "50n", 2),
    (751, "Sand",         "sand", "50d", "50n", 3),
    (761, "Dust",         "dust", "50d", "50n", 2),
    (762, "Ash",          "volcanic ash", "50d", "50n", 5),
    (771, "Squall",       "squalls", "50d", "50n", 4),
    (781, "Tornado",      "tornado", "50d", "50n", 5),
    (800, "Clear",        "clear sky", "01d", "01n", 1),
    (801, "Clouds",       "few clouds", "02d", "02n", 1),
    (802, "Clouds",       "scattered clouds", "03d", "03n", 1),
    (803, "Clouds",       "broken clouds", "04d", "04n", 1),
    (804, "Clouds",       "overcast clouds", "04d", "04n", 1),
]


class DimWeatherConditionBuilder:

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def build(self) -> DataFrame:
        return self.spark.createDataFrame(_CONDITIONS, schema=SCHEMA)

    def write(self, df: DataFrame, path: str) -> None:
        df.write.format("delta").mode("overwrite").save(path)
        logger.info("dim_weather_condition written → %s (%d rows)", path, df.count())
