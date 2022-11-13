from app.calculators.calculate_total import calculate
from test.traffic_count_dataframe import traffic_count_dataframe


def test_calculate_total():

    df_calculated = calculate(traffic_count_dataframe)

    assert df_calculated == 295

    