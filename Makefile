PROJECT_NAME=traffic-analysis-service
VERSION=1.0

build:
	@docker build -t cluster-apache-spark:3.3.1 .

run-app:
	@docker-compose up -d

show-result:
	@docker logs traffic-analysis-service-spark-submit-client-1

build-test:
	@docker build -f Dockerfile.test -t $(PROJECT_NAME):latest .

test: build-test
	@docker run -it --rm $(PROJECT_NAME):latest