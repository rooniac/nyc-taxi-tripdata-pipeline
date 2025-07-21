import os
import sys

# ROOT_DIR
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from utils.postgresql_client import PostgreSQLClient

def create_schemas():
    schemas = ["cdc_source", "staging", "production"]

    client = PostgreSQLClient()
    client.create_conn()

    for schema in schemas:
        query = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        client.execute_query(query)
        print(f"Created schema: {schema}")

    client.close()

if __name__ == "__main__":
    create_schemas()
