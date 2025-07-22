from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from data.download_data import download_tripdata_for_airflow
from pipelines.ingest.ingest_to_raw import ingest_to_raw_for_airflow


with DAG(
    dag_id='nyc_taxi_ingest',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
    }
) as dag:
    download_task = PythonOperator(
        task_id='download_parquet',
        python_callable=download_tripdata_for_airflow,
        provide_context=True,
    )

    ingest_task = PythonOperator(
        task_id='ingest_to_raw',
        python_callable=ingest_to_raw_for_airflow,
        provide_context=True,
    )
    download_task >> ingest_task

