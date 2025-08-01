import great_expectations as gx
from airflow.exceptions import AirflowException
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_ge_checkpoint(**context):
    ge_context = gx.get_context()

    execution_date = context['execution_date'].isoformat()

    batch_request = {
        "datasource_name": "nyc_taxi_db",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "stg_tripdata",
        "runtime_parameters": {
            "query": f"SELECT * FROM staging.stg_tripdata WHERE updated_at >= '{execution_date}'::timestamp - INTERVAL '5 minutes'"   
        },
        "batch_identifiers": {
            "default_identifier_name": f"validate_recent_data_{execution_date.replace(':', '').replace('-', '')[:8]}"
        }
    }

    # get total_rows from XCom
    total_rows = context['ti'].xcom_pull(task_ids='processed_to_staging', key='total_rows')

    if total_rows:
        datasource = ge_context.datasources['nyc_taxi_db']
        connection_string = datasource.execution_engine.connection_string
        if connection_string is None:
            raise ValueError("Connection string not found in datasource configuration")
        
        engine = create_engine(connection_string)
        with engine.connect() as conn:
            result = conn.execute(
                text(f"SELECT COUNT(*) FROM staging.stg_tripdata WHERE updated_at >= '{execution_date}'::timestamp - INTERVAL '5 minutes'")
            )
            ge_row_count = result.scalar()

            # Compare row count
            if total_rows != ge_row_count:
                logging.warning(f"Warning: Expected {total_rows} rows, but GE validated {ge_row_count} rows.")
    
    # Run checkpoint
    checkpoint_result = ge_context.run_checkpoint(
        checkpoint_name="stg_tripdata_checkpoint",
        validations=[{
            "batch_request": batch_request, 
            "expectation_suite_name": "stg_tripdata_suite"
        }]
    )

    if not checkpoint_result["success"]:
        raise AirflowException(f"GE validation failed. Check HTML report at gx/uncommitted/data_docs/local_site/index.html")
    
    logger.info(f"Validation successful. Rows validated: {ge_row_count:,}")
    return {"status": "success", "ge_row_count": ge_row_count if total_rows else None}
