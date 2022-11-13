from pyspark.sql import DataFrame


def calculate(dataframe: DataFrame) -> int:
    return dataframe \
            .rdd \
            .map(lambda x: (1, x[1])) \
            .reduceByKey(lambda x, y: x + y) \
            .collect()[0][1]
