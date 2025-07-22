import os
import sys
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import *
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import json
import logging

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg, get_config_value

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, ".env"))
CFG_FILE = os.path.join(ROOT_DIR, "configs", "datalake.yaml")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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

def init_spark(endpoint, access_key, secret_key):
    logger.info(f"Starting Spark with JAVA_HOME: {os.environ.get('JAVA_HOME')}")
    builder = SparkSession.builder \
        .appName("ConvertParquetToDelta") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # .config("spark.driver.memory", "2g") \
        # .config("spark.executor.memory", "2g") \
        # .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=file:///opt/airflow/log4j2.properties -XX:+PrintGCDetails") \
        # .config("spark.pyspark.python", "python3")

    if os.environ.get('AIRFLOW_HOME'):
        logger.info("Running in Airflow/Docker, using local JARs")
        builder = builder.config(
            "spark.jars",
            "/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
            "/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar,"
            "/opt/airflow/jars/hadoop-common-3.3.4.jar,"
            "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.4.1.jar,"
            "/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.4.1.jar,"
            "/opt/airflow/jars/kafka-clients-3.4.0.jar,"
            "/opt/airflow/jars/commons-pool2-2.11.1.jar,"
            "/opt/airflow/jars/delta-spark_2.12-3.3.2.jar,"
            "/opt/airflow/jars/delta-storage-3.3.2.jar,"
            "/opt/airflow/jars/postgresql-42.7.3.jar"
        )
    else:
        logger.info("Running locally, using spark.jars")

    spark = builder.getOrCreate()
    # logger.info(f"Spark classpath: {spark.sparkContext._jsc.sc().conf().get('spark.driver.extraClassPath')}")
    return spark

def cast_to_standard_schema(df):
    for field in STANDARD_SCHEMA.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, df[field.name].cast(field.dataType))

    # Reorder columns
    return df.select([f.name for f in STANDARD_SCHEMA.fields])

def convert_to_delta_for_airflow(**context):
    try:
        conn = BaseHook.get_connection('minio_default')
        extra = json.loads(conn.extra) if conn.extra else {}
        endpoint = conn.host.replace('http://', '') if conn.host else extra.get('endpoint_url', '').replace('http://', '')
        if not endpoint:
            raise ValueError("MinIO endpoint is not configured in Airflow Connection")
        access_key = conn.login or "minioadmin"
        secret_key = conn.password or "minioadmin123"
        bucket_processed = extra.get('bucket_name', 'processed')
        bucket_sandbox = extra.get('bucket_sandbox', 'sandbox')
        logger.info(f"Using Airflow Connection for MinIO: {endpoint}")
    except Exception as e:
        logger.warning(f"Failed to get Airflow Connection: {str(e)}. Falling back to datalake.yaml")
        cfg = load_cfg(CFG_FILE)
        if cfg is None:
            logger.error("Failed to load configuration. Exiting.")
            raise
        endpoint = get_config_value(cfg, ["datalake", "endpoint"], default="minio:9000")
        access_key = get_config_value(cfg, ["datalake", "access_key"], default="minioadmin")
        secret_key = get_config_value(cfg, ["datalake", "secret_key"], default="minioadmin123")
        bucket_processed = get_config_value(cfg, ["datalake", "buckets", "processed"], default="processed")
        bucket_sandbox = get_config_value(cfg, ["datalake", "buckets", "sandbox"], default="sandbox")

    spark = init_spark(endpoint, access_key, secret_key)
    minio_client = MinIOClient(endpoint, access_key, secret_key)
    minio_client.create_bucket(bucket_sandbox)

    processed_files = Variable.get("delta_processed_files", default_var=[], deserialize_json=True)
    # Cast to list type
    if isinstance(processed_files, str):
        try:
            processed_files = json.loads(processed_files)
            if not isinstance(processed_files, list):
                processed_files = []
        except Exception:
            processed_files = []
    # Debug
    logger.info(f"[DEBUG] processed_files type: {type(processed_files)}, value: {processed_files}")

    logger.info(f"Previously processed files: {processed_files}")

    parquet_files = minio_client.list_parquet_files(bucket_processed, prefix="batch/")
    if not parquet_files:
        logger.info("No parquet files found in processed bucket")
        spark.stop()
        return
    
    logger.info(f"Found {len(parquet_files)} parquet files: {parquet_files}")
    new_files = [f for f in parquet_files if os.path.basename(f) not in processed_files]
    if not new_files:
        logger.info("No new parquet files to process")
        spark.stop()
        return
    
    logger.info(f"Processing {len(new_files)} new files: {new_files}")

    for object_name in new_files:
        file_name = os.path.basename(object_name)
        # year or streaming
        # try:
        #     year = file_name.split("_")[-1].split("-")[0] # green_tripdata_2025-01.parquet -> 2025
        #     delta_path = f"s3a://{bucket_sandbox}/batch/{year}/"
        #     logger.info(f"Processing regular file: {file_name}...")
        # except (IndexError, ValueError):
        #     delta_path = f"s3a://{bucket_sandbox}/batch/streaming/"
        #     logger.info(f"Processing streaming file: {file_name}")
        if "tripdata" in file_name and file_name.endswith(".parquet"):
            year = file_name.split("_")[-1].split("-")[0]        
            delta_path = f"s3a://{bucket_sandbox}/batch/{year}/"
            logger.info(f"Processing regular file: {file_name}...")
        else:
            delta_path = f"s3a://{bucket_sandbox}/batch/streaming/"
            logger.info(f"Processing streaming file: {file_name}")

        parquet_path = f"s3a://{bucket_processed}/{object_name}"
        try:
            delta_exists = bool(minio_client.list_objects(bucket_sandbox, prefix=delta_path.replace(f"s3a://{bucket_sandbox}/", ""), recursive=True))
            logger.info(f"Delta table exists at {delta_path}: {delta_exists}")

            # Read and process file
            logger.info(f"Reading file: {parquet_path}")
            df = spark.read.parquet(parquet_path)
            row_count = df.count()
            logger.info(f"Loaded {row_count} rows from {file_name}")

            df = cast_to_standard_schema(df)

            # Write to sandbox bucket, delta format
            logger.info(f"Converting to delta format: {delta_path}")
            df.write \
                .format("delta") \
                .mode("append") \
                .save(delta_path)
            
            processed_files.append(file_name)
            Variable.set("delta_processed_files", processed_files, serialize_json=True)
            logger.info(f"Successfully converted {file_name} to delta at {delta_path}")
        
        except Exception as e:
            logger.error(f"Failed to processed {file_name}: {str(e)}")
            continue
    
    logger.info("Conversion complete.")
    spark.stop()


def convert_all_parquet_to_delta():
    cfg = load_cfg(CFG_FILE)
    if cfg is None:
        logger.error("Failed to load configuration. Exiting.")
        sys.exit(1)

    endpoint = get_config_value(cfg, ["datalake", "endpoint"])
    access_key = get_config_value(cfg, ["datalake", "access_key"])
    secret_key = get_config_value(cfg, ["datalake", "secret_key"])
    bucket_processed = get_config_value(cfg, ["datalake", "buckets", "processed"], "processed")
    bucket_sandbox = get_config_value(cfg, ["datalake", "buckets", "sandbox"], "sandbox")
    folder_name = get_config_value(cfg, ["datalake", "folder_name"], "batch")

    spark = init_spark(endpoint, access_key, secret_key)
    minio_client = MinIOClient(endpoint, access_key, secret_key)
    minio_client.create_bucket(bucket_sandbox)

    parquet_files = minio_client.list_parquet_files(bucket_processed, prefix=f"{folder_name}/")
    if not parquet_files:
        logger.info("No parquet files found in processed bucket")
        spark.stop()
        return

    logger.info(f"Found {len(parquet_files)} parquet files: {parquet_files}")

    for object_name in parquet_files:
        file_name = os.path.basename(object_name)
        parquet_path = f"s3a://{bucket_processed}/{object_name}"
        delta_path = f"s3a://{bucket_sandbox}/{folder_name}/"

        try:
            logger.info(f"Reading file: {parquet_path}")
            df = spark.read.parquet(parquet_path)
            logger.info(f"Loaded {df.count()} rows from {file_name}")

            df = cast_to_standard_schema(df)

            df.write \
                .format("delta") \
                .mode("append") \
                .save(delta_path)
            
            logger.info(f"Converted to delta format: {delta_path}")

        except Exception as e:
            logger.error(f"Failed to process {file_name}: {str(e)}")
            continue

    logger.info("Conversion complete.")
    spark.stop()

if __name__ == "__main__":
    convert_all_parquet_to_delta()
