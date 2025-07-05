---
title: "Scalable Data Pipeline Case Study"
date: 2025-07-04
tags: [Data Engineering, Airflow, Docker, Python, ETL]
---

# Building Scalable Data Pipelines in Python

A complete case study demonstrating how to extract, transform, and load data using Airflow, Pandas, PostgreSQL, and Docker for environment consistency.

---

## Overview

This pipeline automates a daily ETL process:

1. Extract JSON data from a paginated REST API  
2. Normalize and clean the data with Pandas  
3. Load cleaned data into PostgreSQL via SQLAlchemy  
4. Run analytics queries and export results  
5. Log, monitor, and notify via Airflow and Loguru  
6. Containerize all components with Docker for reproducible deployments

---

## Architecture Diagram

```mermaid
flowchart LR
  A[Airflow Scheduler] -->|Triggers| B[Extract Container]
  B --> C[Transform Container]
  C --> D[Load Container]
  D --> E[(PostgreSQL)]
  D --> F[Analytics Container]
  F --> G[/weekly_summary.csv/]
  A --> H[Email Notification]
```

---

## File Structure

```text 
pipeline_project/
├─ dags/
│  └─ daily_etl_pipeline_docker.py
├─ etl_scripts/
│  ├─ extract.py
│  ├─ transform.py
│  ├─ load.py
│  └─ analytics.py
├─ docker-compose.yml
├─ Dockerfile
├─ requirements.txt
└─ README.md
```

---

## Code Walkthrough

1. Airflow DAG 

```python
# dags/daily_etl_pipeline_docker.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'grant',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    extract = DockerOperator(
        task_id='extract_data',
        image='my-etl-image:latest',
        command='python etl_scripts/extract.py',
        network_mode='bridge'
    )

    transform = DockerOperator(
        task_id='transform_data',
        image='my-etl-image:latest',
        command='python etl_scripts/transform.py',
        network_mode='bridge'
    )

    load = DockerOperator(
        task_id='load_data',
        image='my-etl-image:latest',
        command='python etl_scripts/load.py',
        network_mode='bridge'
    )

    analytics = DockerOperator(
        task_id='run_analytics',
        image='my-etl-image:latest',
        command='python etl_scripts/analytics.py',
        network_mode='bridge'
    )

    email = EmailOperator(
        task_id='email_notification',
        to='team@example.com',
        subject='Pipeline Success',
        html_content='The daily ETL pipeline completed successfully!'
    )

    extract >> transform >> load >> analytics >> email
```

---

2. Extraction Script

```python
# etl_scripts/extract.py
import requests
import pandas as pd

def extract_data():
    base_url = "https://api.example.com/data"
    page = 1
    data = []

    while True:
        resp = requests.get(f"{base_url}?page={page}")
        if resp.status_code != 200:
            break
        results = resp.json().get("results", [])
        if not results:
            break
        data.extend(results)
        page += 1

    df = pd.DataFrame(data)
    df.to_csv("/app/raw_data.csv", index=False)

if __name__ == "__main__":
    extract_data()
```

---

3. Transformation Script

```python
# etl_scripts/transform.py
import pandas as pd

def clean_data():
    df = pd.read_csv("/app/raw_data.csv")
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['amount'] = df['amount'].fillna(0).astype(float)
    df = df[df['status'] == 'active']
    df.to_csv("/app/clean_data.csv", index=False)

if __name__ == "__main__":
    clean_data()
```

---

4. Load Script

```python
# etl_scripts/load.py
import os
import pandas as pd
from sqlalchemy import create_engine

def load_to_db():
    df = pd.read_csv("/app/clean_data.csv")
    db_url = os.getenv("DATABASE_URL", "postgresql://user:pass@postgres:5432/mydb")
    engine = create_engine(db_url)
    df.to_sql("daily_snapshot", engine, if_exists='append', index=False)

if __name__ == "__main__":
    load_to_db()
```

---

5. Analytics Script

```python
# etl_scripts/analytics.py
import os
import pandas as pd
from sqlalchemy import create_engine, text

def run_analytics():
    db_url = os.getenv("DATABASE_URL", "postgresql://user:pass@postgres:5432/mydb")
    engine = create_engine(db_url)
    query = text("""
        SELECT user_id, SUM(amount) AS total_spent
        FROM daily_snapshot
        WHERE created_at >= current_date - interval '7 days'
        GROUP BY user_id
    """)
    with engine.connect() as conn:
        result = conn.execute(query)
        df = pd.DataFrame(result.fetchall(), columns=['user_id','total_spent'])
        df.to_csv("/app/weekly_summary.csv", index=False)

if __name__ == "__main__":
    run_analytics()
```

---

## Docker Integration

requirements.txt

```text
apache-airflow
apache-airflow-providers-docker
pandas
requests
sqlalchemy
psycopg2-binary
loguru
```

Dockerfile

```dockerfile
# Stage 1: build dependencies
FROM python:3.10-slim AS builder
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: final image
FROM python:3.10-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY --from=builder /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
COPY . .
```

docker-compose.yml

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: pass
      POSTGRES_DB: mydb
    volumes:
      - postgres_data:/var/lib/postgresql/data

  airflow:
    image: apache/airflow:2.6.1
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://user:pass@postgres:5432/mydb
      AIRFLOW__CORE__FERNET_KEY: ''
      _PIP_ADDITIONAL_REQUIREMENTS: pandas,requests,sqlalchemy,psycopg2-binary,loguru,apache-airflow-providers-docker
    volumes:
      - ./dags:/opt/airflow/dags
      - /var/run/docker.sock:/var/run/docker.sock
    ports:
      - "8080:8080"
    depends_on:
      - postgres

  etl:
    build: .
    image: my-etl-image:latest
    environment:
      DATABASE_URL: postgresql://user:pass@postgres:5432/mydb
    volumes:
      - ./etl_scripts:/app/etl_scripts

volumes:
  postgres_data:
```

---

## Best Practices and Next Steps

- Pin base images (e.g., python:3.10.12-slim) to avoid drift  
- Secure secrets with environment files or a secret manager  
- Implement CI/CD to build & push images on code changes  
- Use Kubernetes and Helm for auto-scaling Airflow in production  
- Integrate data quality tools (Great Expectations, Deequ)  
- Add observability: metrics dashboards, SLA checks, service catalogs

---
