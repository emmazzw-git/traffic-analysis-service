from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month, dayofmonth, col

# Source Fields
SOURCE_TIME = "start_of_half_hour"

# Target Fields
TARGET_YEAR = "year"
TARGET_MONTH = "month"
TARGET_DAY = "day"


def transform(dataframe: DataFrame) -> DataFrame:
    return dataframe \
        .withColumn(
            TARGET_YEAR,
            year(col(SOURCE_TIME))) \
        .withColumn(
            TARGET_MONTH,
            month(col(SOURCE_TIME))) \
        .withColumn(
            TARGET_DAY,
            dayofmonth(col(SOURCE_TIME)))
