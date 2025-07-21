import os
import sys
import pandas as pd
import numpy as np
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from utils.postgresql_client import PostgreSQLClient

# Load .env
load_dotenv(dotenv_path=os.path.join(ROOT_DIR, ".env"))

PARQUET_FILE = os.path.join(ROOT_DIR, "data", "2025", "green_tripdata_2025-01.parquet")
TARGET_SCHEMA = "cdc_source"
TARGET_TABLE = "green_tripdata"  # or yellow_tripdata
BATCH_SIZE = 10000
LIMIT_ROWS = 1_000_000

def sanitize_row(row):
    """Convert numpy types to native Python types for PostgreSQL compatibility."""
    return tuple(
        int(x) if isinstance(x, (np.integer,)) else
        float(x) if isinstance(x, (np.floating,)) else
        str(x) if isinstance(x, (np.str_,)) else
        None if pd.isna(x) else x
        for x in row
    )

def insert_dataframe_to_postgres(df, table_name, schema_name, client: PostgreSQLClient):
    placeholders = ",".join(["%s"] * len(df.columns))
    columns = ",".join(df.columns)
    values = [sanitize_row(row) for row in df.itertuples(index=False, name=None)]
    insert_query = f'INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})'

    with client.conn.cursor() as cur:
        for i in range(0, len(values), BATCH_SIZE):
            batch = values[i:i + BATCH_SIZE]
            try:
                cur.executemany(insert_query, batch)
                client.conn.commit()
                print(f"Inserted {i + len(batch)} / {len(values)} rows.")
            except Exception as e:
                print(f"Error at batch {i}–{i + BATCH_SIZE}: {e}")
                client.conn.rollback()

def main():
    print(f"Reading Parquet file...{PARQUET_FILE}")
    df = pd.read_parquet(PARQUET_FILE)

    print(f"Selecting first {LIMIT_ROWS} rows...")
    df = df.iloc[:LIMIT_ROWS]
    df.columns = [col.lower() for col in df.columns]

    print("Sanitizing BIGINT columns & converting types...")

    # Connect to PostgreSQL
    pg_client = PostgreSQLClient()
    pg_client.create_conn()

    print(f"Inserting data into {TARGET_SCHEMA}.{TARGET_TABLE}...")
    insert_dataframe_to_postgres(df, TARGET_TABLE, TARGET_SCHEMA, pg_client)

    pg_client.close()
    print(f"Inserted to cdc source {TARGET_TABLE}.")

if __name__ == "__main__":
    main()
