from datetime import datetime
from app.calculators.calculate_least_cars import calculate
from test.processed_traffic_count_dataframe import processed_traffic_count_dataframe
from pyspark.sql import Row

SOURCE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def test_calculate_least_cars():

    df_calculated = calculate(processed_traffic_count_dataframe)

    assert df_calculated == {
        'start_time_list': [
            Row(start_of_half_hour=datetime.strptime(
                "2022-01-02T01:00:00",
                SOURCE_FORMAT
            ),
            total_one_and_half_hours=5
        )],
        'end_time_list': [
            Row(start_of_half_hour=datetime.strptime(
                "2022-01-02T02:30:00",
                SOURCE_FORMAT
            )
        )]
    }