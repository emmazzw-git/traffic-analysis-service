from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, IntegerType

# Target Fields
TARGET_TIME_FIELD = "start_of_half_hour"
TARGET_CAR_NUMBER_FIELD = "car_number"


def read(
    spark: SparkSession,
    input_csv: str,
) -> DataFrame:
    traffic_counter_chema = StructType() \
        .add(TARGET_TIME_FIELD, TimestampType()) \
        .add(TARGET_CAR_NUMBER_FIELD, IntegerType()) 
    
    return spark \
        .read \
        .option("delimiter", " ") \
        .option("header", True) \
        .schema(traffic_counter_chema) \
        .csv(input_csv)
