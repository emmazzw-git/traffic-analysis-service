# Traffic Analysis Service

## Pre requisites
* Docker installed
* Docker compose  installed

## Run the app and test

### Build the image

```sh
make build
```

### Run the app

```sh
make run-app
```

### See the result in docker log

```sh
make show-result
```

```
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
===========================================================
1. The number of cars seen in total: 398
-----------------------------------------------------------
2. A sequence of lines showing yyyy-mm-dd NumberOfCars:
2021-12-01 179
2021-12-05 81
2021-12-08 134
2021-12-09 4
-----------------------------------------------------------
3. The top 3 half hours with most cars:
2021-12-01T07:30:00
2021-12-01T08:00:00
2021-12-08T18:00:00
-----------------------------------------------------------
4. The 1.5 hour period with least cars:
2021-12-01 23:30:00 to 2021-12-02 01:00:00
===========================================================
+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
```

### Run unit test

```sh
make test
```

# What's left to do?

* add lint
* add mypy type check
* add Spark UI for performance analysis
* add error handling such as exception catching
* add unit test coverage and improve unit test coverage
* add [Integration Test](https://getindata.com/blog/integration-tests-spark-applications-big-data/)
* add CI/CD
* add deployment step to CI/CD pipeline