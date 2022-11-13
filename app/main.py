import sys
from pyspark.sql import SparkSession

import reader.csv_reader as csv_source
from transformers \
    import (
      add_time_columns,
      add_next_traffic_number_columns
    )

from calculators \
    import (
      calculate_total as total,
      calculate_top_three_car_number as top_three_car_number_calculator,
      calculate_total_in_day as total_in_day_calculator,
      calculate_least_cars as least_cars_calculator
    )

from writers \
    import (
      write_top_three_car_number as top_three_car_number_writer,
      write_total_in_day as total_in_day_writer,
      write_least_cars as least_cars_writer
    )


def run(spark: SparkSession, input_csv: str):
  traffic_count_df = csv_source.read(
    spark,
    input_csv
  )

  traffic_count_df.show(5, truncate=False)

  processed_traffic_count_df = traffic_count_df \
    .transform(add_next_traffic_number_columns.transform) \
    .transform(add_time_columns.transform)

  # Q1. The number of cars seen in total
  total_number_of_cars = total \
    .calculate(traffic_count_df)

  # Q2. A sequence of lines showing yyyy-mm-dd NumberOfCars
  total_car_number_in_day_list = total_in_day_calculator \
    .calculate(processed_traffic_count_df)

  # Q3. The top 3 half hours with most cars 
  top3_half_hours_most_cars_list = top_three_car_number_calculator \
    .calculate(traffic_count_df)

  # Q4. The 1.5 hour period with least cars
  least_cars_lists = least_cars_calculator \
    .calculate(processed_traffic_count_df)

  # processed_traffic_count_df \
  #   .show(
  #     processed_traffic_count_df.count(),
  #     truncate=False
  #   )

  print(f"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
  print(f"===========================================================")
  print(f"1. The number of cars seen in total: {total_number_of_cars}")
  print(f"-----------------------------------------------------------")
  print(f"2. A sequence of lines showing yyyy-mm-dd NumberOfCars:")
  total_in_day_writer.write(total_car_number_in_day_list)
  print(f"-----------------------------------------------------------")
  print(f"3. The top 3 half hours with most cars:")
  top_three_car_number_writer.write(top3_half_hours_most_cars_list)
  print(f"-----------------------------------------------------------")
  print(f"4. The 1.5 hour period with least cars:")
  least_cars_writer.write(least_cars_lists)
  print(f"===========================================================")
  print(f"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


if __name__ == "__main__":
  args = sys.argv
  print(f'args are {args}')

  if len(args) < 4 and None in args:
    sys.exit("You need to submit the app with all the required params")

  stage = args[1]
  app_name = args[2]
  input_csv = args[3]

  spark = SparkSession \
    .builder \
    .master(stage) \
    .appName(app_name) \
    .getOrCreate()
  
  run(spark, input_csv)
