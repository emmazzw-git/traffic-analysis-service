from datetime import datetime
from test.session_wrapper import spark

SOURCE_FORMAT = "%Y-%m-%dT%H:%M:%S"

columns = [
    "start_of_half_hour",
    "car_number"
]

data = [
    (
        datetime.strptime(
            "2022-01-01T09:00:00",
            SOURCE_FORMAT
        ),
        20
    ),
    (
        datetime.strptime(
            "2022-01-01T10:00:00",
            SOURCE_FORMAT
        ),
        20
    ),
    (
        datetime.strptime(
            "2022-01-01T11:00:00",
            SOURCE_FORMAT
        ),
        30
    ),
    (
        datetime.strptime(
            "2022-01-01T13:00:00",
            SOURCE_FORMAT
        ),
        50
    ),
    (
        datetime.strptime(
            "2022-01-01T13:30:00",
            SOURCE_FORMAT
        ),
        60
    ),
    (
        datetime.strptime(
            "2022-01-01T14:00:00",
            SOURCE_FORMAT
        ),
        70
    ),
    (
        datetime.strptime(
            "2022-01-01T16:00:00",
            SOURCE_FORMAT
        ),
        10
    ),
    (
        datetime.strptime(
            "2022-01-01T20:00:00",
            SOURCE_FORMAT
        ),
        20
    ),
    (
        datetime.strptime(
            "2022-01-01T23:00:00",
            SOURCE_FORMAT
        ),
        10
    ),
    (
        datetime.strptime(
            "2022-01-02T01:00:00",
            SOURCE_FORMAT
        ),
        5
    ),
]

traffic_count_dataframe = spark.createDataFrame(data, columns)