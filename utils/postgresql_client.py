import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(dotenv_path=os.path.join(ROOT_DIR, ".env"))

POSTGRES_DB=os.getenv("POSTGRES_DB")
POSTGRES_USER=os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD=os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST=os.getenv("POSTGRES_HOST")
POSTGRES_PORT=os.getenv("POSTGRES_PORT")

class PostgreSQLClient:
    def __init__(self):
        self.dbname=POSTGRES_DB
        self.user=POSTGRES_USER
        self.password=POSTGRES_PASSWORD
        self.host=POSTGRES_HOST
        self.port=POSTGRES_PORT
        self.conn = None
    
    def create_conn(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            )
            print("PostgreSQL connection established.")
        except Exception as e:
            print(f"Error connecting to PostgreSQL: {e}")
            raise

    def execute_query(self, query: str):
        if not self.conn:
            self.create_conn()

        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                self.conn.commit()
                print("Query executed successfully.")
        except Exception as e:
            print(f"Failed to execute query: {e}")
            self.conn.rollback()
            raise

    def get_columns(self, schema: str, table: str):
        if not self.conn:
            self.create_conn()

        try:
            with self.conn.cursor() as cur:
                cur.execute(sql.SQL("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                """), (schema, table))
                return [row[0] for row in cur.fetchall()]
        except Exception as e:
            print(f"Failed to get columns: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            print("PostgreSQL connection closed.")