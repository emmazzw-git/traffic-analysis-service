from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp, lead, when, lit

# Source Fields
SOURCE_TIME_FIELD = "start_of_half_hour"
SOURCE_CAR_NUMBER_FIELD = "car_number"
SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss"
SOURCE_NEXT_LEAD_NUM = 1
SOURCE_NEXT_TWO_LEAD_NUM = 2
SOURCE_MAX_TIME_DIFF = 1

# Target Fields
TARGET_NEXT_COUNT_CAR_NUMBER= "next_count_car_number"
TARGET_NEXT_TWO_COUNTS_CAR_NUMBER= "next_two_counts_car_number"


def transform(dataframe: DataFrame) -> DataFrame:
    return dataframe \
        .withColumn(
            TARGET_NEXT_COUNT_CAR_NUMBER,
            get_traffic_number_count(
                SOURCE_TIME_FIELD,
                SOURCE_CAR_NUMBER_FIELD,
                SOURCE_FORMAT,
                SOURCE_NEXT_LEAD_NUM,
                SOURCE_MAX_TIME_DIFF
            )
        ) \
        .withColumn(
            TARGET_NEXT_TWO_COUNTS_CAR_NUMBER,
            get_traffic_number_count(
                SOURCE_TIME_FIELD,
                SOURCE_CAR_NUMBER_FIELD,
                SOURCE_FORMAT,
                SOURCE_NEXT_TWO_LEAD_NUM,
                SOURCE_MAX_TIME_DIFF
            )
        )


def get_traffic_number_count(
    time_field: str,
    car_number_field: str,
    format: str,
    lead_num: int,
    max_time_diff: int
):
    window = Window.orderBy(time_field)
    current_unix_time = unix_timestamp(
        time_field,
        format
    )
    next_unix_time = unix_timestamp(
        lead(
            time_field,
            lead_num
        ) \
        .over(
            window
        ),
        format
    )
    time_diff_in_hour = (next_unix_time - current_unix_time)/60/60
    car_number = when(
                    time_diff_in_hour <= max_time_diff,
                    lead(
                        car_number_field,
                        lead_num
                    ) \
                    .over(window)
                 ) \
                 .otherwise(lit(0))
    return car_number
