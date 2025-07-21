import os
import sys

# ROOT_DIR
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from utils.postgresql_client import PostgreSQLClient

def create_staging_table():
    client = PostgreSQLClient()
    client.create_conn()

    create_schema = """
        CREATE SCHEMA IF NOT EXISTS staging;
    """

    stg_tripdata_table = """
        CREATE TABLE IF NOT EXISTS staging.stg_tripdata (
            trip_key TEXT PRIMARY KEY,
            vendorid INTEGER,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE PRECISION,
            trip_distance DOUBLE PRECISION,
            ratecodeid DOUBLE PRECISION,
            pulocationid INTEGER,
            dolocationid INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            congestion_surcharge DOUBLE PRECISION,
            pickup_latitude DOUBLE PRECISION,
            pickup_longitude DOUBLE PRECISION,
            dropoff_latitude DOUBLE PRECISION,
            dropoff_longitude DOUBLE PRECISION,
            service_type INTEGER,
            pickup_borough TEXT,
            pickup_zone TEXT,
            dropoff_borough TEXT,
            dropoff_zone TEXT,
            day_part_id TEXT,
            update_at TIMESTAMP
        );
    """    

    stg_load_log_table = """
        CREATE TABLE IF NOT EXISTS staging.stg_loadlog (
            file_name TEXT PRIMARY KEY,
            load_time TIMESTAMP
        );
    """

    for query in [create_schema, stg_tripdata_table, stg_load_log_table]:
        client.execute_query(query)
    
    print("Created staging.stg_tripdata, staging.stg_loadlog table")
    client.close()

if __name__ == "__main__":
    create_staging_table()

