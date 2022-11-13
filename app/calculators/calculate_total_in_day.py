from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum

# Source Fields
SOURCE_CAR_NUMBER_FIELD = "car_number"
SOURCE_YEAR = "year"
SOURCE_MONTH = "month"
SOURCE_DAY = "day"
SOURCE_CAR_TOTAL_FIELD = "total_car_number"


def calculate(dataframe: DataFrame) -> list:
    df = get_total_in_day_df(dataframe)
    return df.rdd.collect()


def get_total_in_day_df(dataframe: DataFrame) -> DataFrame:
    return dataframe \
            .groupBy(
                SOURCE_YEAR,
                SOURCE_MONTH,
                SOURCE_DAY
            ) \
            .agg(
                sum(SOURCE_CAR_NUMBER_FIELD)
                .alias(SOURCE_CAR_TOTAL_FIELD)
            ) \
            .orderBy(
                col(SOURCE_YEAR).asc(),
                col(SOURCE_MONTH).asc(),
                col(SOURCE_DAY).asc()
            )
