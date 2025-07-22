from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.transform.transform_data import transform_data_for_airflow

with DAG(
    dag_id='nyc_taxi_transform',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
    }
) as dag:
    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable=transform_data_for_airflow,
    )
    