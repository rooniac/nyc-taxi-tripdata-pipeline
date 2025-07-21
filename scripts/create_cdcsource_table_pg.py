import os
import sys

# ROOT_DIR
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from utils.postgresql_client import PostgreSQLClient

def create_tables():
    client = PostgreSQLClient()
    client.create_conn()

    yellow_table = """
        CREATE TABLE IF NOT EXISTS cdc_source.yellow_tripdata (
            VendorID INTEGER,
            tpep_pickup_datetime TIMESTAMP,
            tpep_dropoff_datetime TIMESTAMP,
            passenger_count BIGINT,
            trip_distance DOUBLE PRECISION,
            RatecodeID BIGINT,
            store_and_fwd_flag TEXT,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            payment_type BIGINT,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            congestion_surcharge DOUBLE PRECISION,
            Airport_fee DOUBLE PRECISION,
            cbd_congestion_fee DOUBLE PRECISION
        );
        """

    green_table = """
        CREATE TABLE IF NOT EXISTS cdc_source.green_tripdata (
            VendorID INTEGER,
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            store_and_fwd_flag TEXT,
            RatecodeID BIGINT,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count BIGINT,
            trip_distance DOUBLE PRECISION,
            fare_amount DOUBLE PRECISION,
            extra DOUBLE PRECISION,
            mta_tax DOUBLE PRECISION,
            tip_amount DOUBLE PRECISION,
            tolls_amount DOUBLE PRECISION,
            ehail_fee DOUBLE PRECISION,
            improvement_surcharge DOUBLE PRECISION,
            total_amount DOUBLE PRECISION,
            payment_type BIGINT,
            trip_type BIGINT,
            congestion_surcharge DOUBLE PRECISION,
            cbd_congestion_fee DOUBLE PRECISION
        );
        """

    for query in [yellow_table, green_table]:
        client.execute_query(query)

    print("Created all tables.")
    client.close()

if __name__ == "__main__":
    create_tables()
