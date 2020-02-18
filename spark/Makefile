SHELL = /bin/bash

.PHONY = module clean

TOPDIR := $(shell git rev-parse --show-toplevel)
IMG_NAME := spark
NAME := spark

build:
	@echo Building Spark Docker Image
	@docker build --rm -t $(IMG_NAME) .


run:
	@echo Running Spark Container
	@docker run -it --name $(NAME) -v $(TOPDIR)/sample-data/:/data/ -v $(TOPDIR)/spark/:/scripts/ --entrypoint "bin/spark-submit" $(IMG_NAME) /scripts/CSV-Parquet-Convertor.py

clean:
	@echo Cleaning Spark Image
	@docker rmi -f $(IMG_NAME)
