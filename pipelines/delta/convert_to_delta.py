import os
import sys
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg, get_config_value

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, ".env"))
CFG_FILE = os.path.join(ROOT_DIR, "configs", "datalake.yaml")

cfg = load_cfg(CFG_FILE)
if cfg is None:
    print("Failed to load configuration. Exiting.")
    sys.exit(1)

endpoint_url = get_config_value(cfg, ["datalake", "endpoint"])
access_key = get_config_value(cfg, ["datalake", "access_key"])
secret_key = get_config_value(cfg, ["datalake", "secret_key"])

BUCKET_PROCESSED = get_config_value(cfg, ["datalake", "buckets", "processed"], "processed")
BUCKET_SANDBOX = get_config_value(cfg, ["datalake", "buckets", "sandbox"], "sandbox")
FOLDER_NAME = get_config_value(cfg, ["datalake", "folder_name"], "batch")

STANDARD_SCHEMA = StructType([
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("dolocationid", LongType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("payment_type", LongType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pulocationid", LongType(), True),
    StructField("ratecodeid", DoubleType(), True),
    StructField("service_type", LongType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("vendorid", LongType(), True),
])

spark = SparkSession.builder \
    .appName("ConvertParquetToDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint_url}") \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

def cast_to_standard_schema(df):
    for field in STANDARD_SCHEMA.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, df[field.name].cast(field.dataType))

    # Reorder columns
    return df.select([f.name for f in STANDARD_SCHEMA.fields])

def convert_all_parquet_to_delta():
    minio_client = MinIOClient(endpoint_url, access_key, secret_key)

    minio_client.create_bucket(BUCKET_SANDBOX)

    parquet_files = minio_client.list_parquet_files(BUCKET_PROCESSED, prefix=FOLDER_NAME + "/")

    if not parquet_files:
        print("No parquet files found in processed bucket")
        return
    print(f"Found {len(parquet_files)} parquet files need to convert... \n")

    for object_name in parquet_files:
        file_name = os.path.basename(object_name)
        parquet_path = f"s3a://{BUCKET_PROCESSED}/{object_name}"
        delta_path = f"s3a://{BUCKET_SANDBOX}/{FOLDER_NAME}/"

        try:
            print(f"Reading file: {parquet_path}")
            df = spark.read.parquet(parquet_path)
            print(f"Loaded {df.count()} rows from {file_name}")

            # Cast to standard schema
            df = cast_to_standard_schema(df)

            df.write \
                .format("delta") \
                .mode("append") \
                .save(delta_path)
        
            print(f"Converted to delta format: {delta_path}")
            print("-" * 80)
        
        except AnalysisException as e:
            print(f"Failed to read/write file {file_name}: {str(e)}")
            sys.exit(1)

        except Exception as e:
            print(f"Unexpected error processing {file_name}: {str(e)}")
            sys.exit(1)
            
    print("Convertion complete.")

if __name__ == "__main__":
    convert_all_parquet_to_delta()
    spark.stop()
