def write(output: dict):
    for x in output["start_time_list"]:
        for y in output["end_time_list"]:
            print(f"{x[0]} to {y[0]}")
