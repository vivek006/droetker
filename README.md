
# Data extractor
ETL for data extractor from a URL and Orchastration through Airflow

## Requirements
1. Python3.7 or latest
2. Docker and docker-compose
3. DBeaver

## Installation
1. Go to droetker folder
2. run postgres db for dwh :
`docker-compose -f docker-compose-postgressql.yml up -d `
3. build docker image :
`docker build  -t "droetker/airflow:latest"`
4. run Airflow image :
`docker-compose up -d`
5. login to postgres and create a DB "demo_data"

## Local Setup and Validation
1. You can check the local airflow over web browser : http://localhost:8080/
2. Login to postgres db for data
