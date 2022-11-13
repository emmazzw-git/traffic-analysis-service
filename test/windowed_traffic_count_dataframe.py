from datetime import datetime
from test.session_wrapper import spark
from pyspark.sql.types import StructType, StructField, TimestampType, LongType

SOURCE_FORMAT = "%Y-%m-%dT%H:%M:%S"

data = [
    (
        datetime.strptime(
            "2022-01-01T09:00:00",
            SOURCE_FORMAT
        ),
        20,
        20,
        0
    ),
    (
        datetime.strptime(
            "2022-01-01T10:00:00",
            SOURCE_FORMAT
        ),
        20,
        30,
        0
    ),
    (
        datetime.strptime(
            "2022-01-01T11:00:00",
            SOURCE_FORMAT
        ),
        30,
        0,
        0
    ),
    (
        datetime.strptime(
            "2022-01-01T13:00:00",
            SOURCE_FORMAT
        ),
        50,
        60,
        70
    ),
    (
        datetime.strptime(
            "2022-01-01T13:30:00",
            SOURCE_FORMAT
        ),
        60,
        70,
        0
    ),
    (
        datetime.strptime(
            "2022-01-01T14:00:00",
            SOURCE_FORMAT
        ),
        70,
        0,
        0
    ),
    (
        datetime.strptime(
            "2022-01-01T16:00:00",
            SOURCE_FORMAT
        ),
        10,
        0,
        0
    ),
    (
        datetime.strptime(
            "2022-01-01T20:00:00",
            SOURCE_FORMAT
        ),
        20,
        0,
        0
    ),
    (
        datetime.strptime(
            "2022-01-01T23:00:00",
            SOURCE_FORMAT
        ),
        10,
        0,
        0
    ),
    (
        datetime.strptime(
            "2022-01-02T01:00:00",
            SOURCE_FORMAT
        ),
        5,
        0,
        0
    ),
]

schema = StructType([
    StructField("start_of_half_hour", TimestampType()),
    StructField("car_number", LongType()),
    StructField("next_count_car_number", LongType()),
    StructField("next_two_counts_car_number", LongType())
])

windowed_traffic_count_dataframe = spark.createDataFrame(data, schema)