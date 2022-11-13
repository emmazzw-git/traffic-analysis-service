from datetime import datetime

SOURCE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"


def write(output: list):
    for x in output:
        print(f"{datetime.strftime(x[0], SOURCE_TIME_FORMAT)}")
