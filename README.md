# Scalable Data Pipeline

## Quickstart

1) Build & start:
   docker-compose up --build

2) Initialize Airflow DB & user:
   docker-compose exec airflow airflow db init
   docker-compose exec airflow airflow users create \
     --username admin --firstname Admin --lastname User \
     --role Admin --email admin@example.com

3) Access UI at http://localhost:8080  
   Trigger "daily_etl_pipeline" or wait for the next run.

## Tear down
docker-compose down -v
