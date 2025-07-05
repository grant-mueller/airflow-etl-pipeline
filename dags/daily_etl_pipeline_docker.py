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
