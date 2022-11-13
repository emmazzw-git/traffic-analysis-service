from app.calculators.calculate_total_in_day import calculate
from test.processed_traffic_count_dataframe import processed_traffic_count_dataframe
from pyspark.sql import Row


def test_calculate_total_in_day():

    df_calculated = calculate(processed_traffic_count_dataframe)

    assert df_calculated == [
        Row(year=2022, month=1, day=1, total_car_number=290),
        Row(year=2022, month=1, day=2, total_car_number=5)
    ]