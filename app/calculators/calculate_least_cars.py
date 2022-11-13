from pyspark.sql import DataFrame
from pyspark.sql.functions import col, unix_timestamp

# Source Fields
SOURCE_TIME_FIELD = "start_of_half_hour"
SOURCE_CAR_NUMBER_FIELD = "car_number"
SOURCE_NEXT_COUNT_CAR_NUMBER_FIELD = "next_count_car_number"
SOURCE_NEXT_TWO_COUNT_CAR_NUMBER_FIELD = "next_two_counts_car_number"
SOURCE_FORMAT_TIMESTAMP = "timestamp"
SOURCE_TIME_PERIOD = 1.5

# Target Fields
TARGET_TOTAL_ONE_AND_HALF_HOURS = "total_one_and_half_hours"

def calculate(dataframe: DataFrame) -> dict:
    start_time_df = get_least_cars_start_time_df(dataframe)
    end_time_df = get_least_cars_end_time_df(start_time_df)

    return {
        'start_time_list': start_time_df.rdd.collect(),
        'end_time_list': end_time_df.rdd.collect()
    }


def get_least_cars_start_time_df(dataframe: DataFrame) -> DataFrame:
    return dataframe \
            .select(
                col(SOURCE_TIME_FIELD),
                (col(SOURCE_CAR_NUMBER_FIELD) + \
                col(SOURCE_NEXT_COUNT_CAR_NUMBER_FIELD) + \
                col(SOURCE_NEXT_TWO_COUNT_CAR_NUMBER_FIELD)) \
                .alias(TARGET_TOTAL_ONE_AND_HALF_HOURS)
            ) \
            .orderBy(
                col(TARGET_TOTAL_ONE_AND_HALF_HOURS).asc(),
            ) \
            .limit(1)


def get_least_cars_end_time_df(start_time_df: DataFrame) -> DataFrame:
    one_and_half_hours_in_seconds = SOURCE_TIME_PERIOD * 60 * 60
    return start_time_df \
            .select(
                (
                    unix_timestamp(
                        col(SOURCE_TIME_FIELD)
                    ) + one_and_half_hours_in_seconds
                )
                .cast(SOURCE_FORMAT_TIMESTAMP)
            ) \
            .limit(1)
