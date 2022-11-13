from datetime import datetime
from app.calculators.calculate_top_three_car_number import calculate
from test.traffic_count_dataframe import traffic_count_dataframe
from pyspark.sql import Row

SOURCE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def test_calculate_top_three_car_number():

    df_calculated = calculate(traffic_count_dataframe)

    assert df_calculated == [
        Row(start_of_half_hour=datetime.strptime(
                "2022-01-01T14:00:00",
                SOURCE_FORMAT
            ),
            car_number=70
        ),
        Row(start_of_half_hour=datetime.strptime(
                "2022-01-01T13:30:00",
                SOURCE_FORMAT
            ),
            car_number=60
        ),
        Row(start_of_half_hour=datetime.strptime(
                "2022-01-01T13:00:00",
                SOURCE_FORMAT
            ),
            car_number=50
        )
    ]