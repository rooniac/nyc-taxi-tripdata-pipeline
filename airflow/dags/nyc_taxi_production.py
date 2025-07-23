from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='nyc_taxi_production',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 7, 1),
    catchup=False,
) as dag:
    # Task run dbt seed
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        retries=0,
        bash_command='dbt seed --profiles-dir /opt/airflow/dbt_tripdata/dbt --project-dir /opt/airflow/dbt_tripdata',
    )

    # Task dbt run models production
    dbt_run = BashOperator(
        task_id='dbt_run',
        retries=0,
        bash_command='dbt run --profiles-dir /opt/airflow/dbt_tripdata/dbt --project-dir /opt/airflow/dbt_tripdata --select production'
    )

    dbt_seed >> dbt_run