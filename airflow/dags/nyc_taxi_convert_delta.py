from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.delta.convert_to_delta import convert_to_delta_for_airflow

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    dag_id='nyc_taxi_convert_delta',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args
) as dag:
    convert_task = PythonOperator(
        task_id='convert_to_delta',
        python_callable=convert_to_delta_for_airflow,
        retries=0,
    )

    convert_task