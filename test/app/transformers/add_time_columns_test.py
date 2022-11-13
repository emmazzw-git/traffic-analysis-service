from app.transformers.add_time_columns import transform
from test.windowed_traffic_count_dataframe import windowed_traffic_count_dataframe
from test.processed_traffic_count_dataframe import processed_traffic_count_dataframe
from chispa.dataframe_comparer import assert_df_equality


def test_add_time_columns():

    df_transformed = transform(windowed_traffic_count_dataframe)

    df_expected = processed_traffic_count_dataframe

    assert_df_equality(df_transformed, df_expected)