from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.datalake_to_dw.processed_to_staging import processed_to_staging_for_airfow

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    dag_id='nyc_taxi_staging',
    schedule_interval=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    load_to_staging = PythonOperator(
        task_id='processed_to_staging',
        python_callable=processed_to_staging_for_airfow,
        retries=0,
    )

    load_to_staging