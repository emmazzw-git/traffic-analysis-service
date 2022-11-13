from app.transformers.add_next_traffic_number_columns import transform
from test.traffic_count_dataframe import traffic_count_dataframe
from test.windowed_traffic_count_dataframe import windowed_traffic_count_dataframe
from chispa.dataframe_comparer import assert_df_equality


def test_add_next_traffic_number_columns():

    df_transformed = transform(traffic_count_dataframe)

    df_expected = windowed_traffic_count_dataframe

    assert_df_equality(df_transformed, df_expected)


