from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# Source Fields
SOURCE_TIME_FIELD = "start_of_half_hour"
SOURCE_CAR_NUMBER_FIELD = "car_number"
SOURCE_CAR_NUMBER_SELECTED = 3


def calculate(dataframe: DataFrame) -> list:
    df = get_top_three_car_number_df(dataframe)
    return df.rdd.collect()


def get_top_three_car_number_df(dataframe: DataFrame) -> DataFrame:
    return dataframe \
            .select(
                col(SOURCE_TIME_FIELD),
                col(SOURCE_CAR_NUMBER_FIELD)
            ) \
            .orderBy(
                col(SOURCE_CAR_NUMBER_FIELD).desc(),
            ) \
            .limit(SOURCE_CAR_NUMBER_SELECTED)
