import os
import sys
import gc
import json
import logging
import time
from dotenv import load_dotenv
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ROOT_DIR  
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from utils.helpers import load_cfg, get_config_value
from utils.minio_utils import MinIOClient

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load config
CFG_FILE = os.path.join(ROOT_DIR, "configs", "datalake.yaml")

# Lookup path
ZONE_LOOKUP_PATH = os.path.join(ROOT_DIR, "pipelines", "data", "taxi_zone_lookup.csv")

def init_spark(endpoint, access_key, secret_key):
    logger.info(f"Starting Spark with JAVA_HOME: {os.environ.get('JAVA_HOME')}")
    builder = SparkSession.builder \
        .appName("ProcessedToStaging") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
    return builder.getOrCreate()


def load_zone_lookup(spark):
    return spark.read.option("header", True).csv(ZONE_LOOKUP_PATH)\
        .select(
            col("LocationID").cast("long"),
            col("Borough"),
            col("zone")
        )

def enrich_with_zone_and_daypart(df, zone_df):
    df = df.join(zone_df.withColumnRenamed("LocationID", "pulocationid"), on="pulocationid", how="left")\
           .withColumnRenamed("Borough", "pickup_borough")\
           .withColumnRenamed("zone", "pickup_zone")

    df = df.join(zone_df.withColumnRenamed("LocationID", "dolocationid"), on="dolocationid", how="left")\
           .withColumnRenamed("Borough", "dropoff_borough")\
           .withColumnRenamed("zone", "dropoff_zone")

    df = df.withColumn("day_part_id", 
        when((hour("pickup_datetime") >= 6) & (hour("pickup_datetime") < 10), lit("morning_rush"))
        .when((hour("pickup_datetime") >= 10) & (hour("pickup_datetime") < 16), lit("mid_day"))
        .when((hour("pickup_datetime") >= 16) & (hour("pickup_datetime") < 20), lit("evening_rush"))
        .otherwise(lit("night"))
    )

    return df

def apply_business_filters(df):
    return df.filter((col("passenger_count") >= 1) & (col("passenger_count") <= 6))\
             .filter((col("trip_distance") > 0) & (col("trip_distance") <= 100))

def apply_iqr_filter(df):
    q1, q3 = df.approxQuantile("total_amount", [0.25, 0.75], 0.01)
    iqr = q3 - q1
    return df.filter((col("total_amount") >= q1 - 1.5 * iqr) & (col("total_amount") <= q3 + 1.5 * iqr))

def deduplicate(df):
    dedup_cols = [
        "vendorid", "pickup_datetime", "dropoff_datetime",
        "pulocationid", "dolocationid", "ratecodeid", "total_amount"
    ]
    return df.dropDuplicates(dedup_cols)

def add_trip_key(df):
    return df.withColumn("trip_key", concat_ws("_",
        col("vendorid").cast("string"),
        col("pickup_datetime").cast("string"),
        col("dropoff_datetime").cast("string"),
        col("pulocationid").cast("string"),
        col("dolocationid").cast("string"),
        col("ratecodeid").cast("string"),
        col("total_amount").cast("string")
    )).withColumn("updated_at", current_timestamp())

def insert_to_postgres(spark, df, file_name, jdbc_url, pg_schema, pg_table, pg_log_table, pg_user, pg_password):
    df.repartition(4).write.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable", f"{pg_schema}.{pg_table}")\
        .option("user", pg_user)\
        .option("password", pg_password)\
        .option("driver", "org.postgresql.Driver")\
        .option("batchsize", 10000)\
        .option("numPartitions", 4)\
        .option("isolationLevel", "NONE")\
        .mode("append")\
        .save()
    log_df = spark.createDataFrame([(file_name, datetime.now())], ["file_name", "load_time"])
    log_df.write.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable", f"{pg_schema}.{pg_log_table}")\
        .option("user", pg_user)\
        .option("password", pg_password)\
        .option("driver", "org.postgresql.Driver")\
        .mode("append")\
        .save()

def processed_to_staging():
    cfg = load_cfg(CFG_FILE)
    if not cfg:
        logger.error("Failed to load datalake config.")
        raise RuntimeError("Failed to load datalake config.")
    
    endpoint = get_config_value(cfg, ["datalake", "endpoint"])
    access_key = get_config_value(cfg, ["datalake", "access_key"])
    secret_key = get_config_value(cfg, ["datalake", "secret_key"])
    bucket_processed = get_config_value(cfg, ["datalake", "buckets", "processed"])
    folder_name = get_config_value(cfg, ["datalake", "folder_name"], "batch")
    
    pg_db = os.getenv("POSTGRES_DB")
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_host = os.getenv("POSTGRES_HOST")
    pg_port = os.getenv("POSTGRES_PORT")
    pg_schema = os.getenv("POSTGRES_SCHEMA_STAGING")
    pg_table = os.getenv("POSTGRES_STAGING_TABLE")
    pg_log_table = os.getenv("POSTGRES_STAGING_LOG")
    jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
    
    spark = init_spark(endpoint, access_key, secret_key)
    minio_client = MinIOClient(endpoint_url=endpoint, access_key=access_key, secret_key=secret_key)
    
    zone_df = load_zone_lookup(spark).cache()
    parquet_files = minio_client.list_parquet_files(bucket_processed, prefix=folder_name + "/")
    
    if not parquet_files:
        logger.info("No files found in processed bucket.")
        spark.stop()
        return

    logger.info(f"Found {len(parquet_files)} parquet files to load.")

    processed_files = spark.read.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"{pg_schema}.{pg_table}") \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .load() \
        .select("file_name") \
        .rdd.flatMap(lambda x: x).collect()
    
    unprocessed_files = [f for f in parquet_files if os.path.basename(f) not in processed_files]
    if not unprocessed_files:
        logger.info("No new files to process")
        zone_df.unpersist(blocking=True)
        spark.stop()
        return
    
    logger.info(f"Processing {len(unprocessed_files)} new parquet files")

    for object_name in unprocessed_files:
        file_name = os.path.basename(object_name)
        parquet_path = f"s3a://{bucket_processed}/{object_name}"
        logger.info(f"Processing file {file_name} ...")

        try:
            start_time = time.time()
            df = spark.read.parquet(parquet_path)
            count_before = df.count()

            df = enrich_with_zone_and_daypart(df, zone_df)
            df = apply_business_filters(df)
            df = apply_iqr_filter(df)
            df = deduplicate(df)
            df = add_trip_key(df)

            count_after = df.count()
            logger.info(f"Row adter processing: {count_after:,} (from {count_before:,})")
            insert_to_postgres(spark, df, file_name, jdbc_url, pg_schema, pg_table, pg_log_table, pg_user, pg_password)
            logger.info(f"Inserted {file_name} to staging in {time.time() - start_time:.2f} seconds")
            
            # Memory cleanup
            del df
            gc.collect()
        except Exception as e:
            logger.error(f"Failed to process {file_name}: {str(e)}")
            raise
        
    zone_df.unpersist(blocking=True)
    logger.info("All files processed")
    spark.stop()

def processed_to_staging_for_airfow(**context):
    try:
        conn = BaseHook.get_connection('minio_default')
        extra = json.loads(conn.extra) if conn.extra else {}
        endpoint = conn.host.replace('http://', '') if conn.host else extra.get('endpoint_url', '').replace('http://', '')
        if not endpoint:
            raise ValueError("MinIO endpoint is not configured in Airflow Connection")
        access_key = conn.login
        secret_key = conn.password
        bucket_processed = extra.get('bucket_processed', 'processed')
        folder_name = extra.get('folder_name', 'batch')
        logger.info(f"Using Airflow Connection for MinIO: {endpoint}")
    except Exception as e:
        logger.warning(f"Failed to get MinIO Airflow Connection: {str(e)}. Falling back to datalake.yaml")
        cfg = load_cfg(CFG_FILE)
        if cfg is None:
            logger.error("Failed to load configuration. Exiting.")
            raise AirflowException("Failed to load configuration.")
        endpoint = get_config_value(cfg, ["datalake", "endpoint"])
        access_key = get_config_value(cfg, ["datalake", "access_key"])
        secret_key = get_config_value(cfg, ["datalake", "secret_key"])
        bucket_processed = get_config_value(cfg, ["datalake", "buckets", "processed"])
        folder_name = get_config_value(cfg, ["datalake", "folder_name"], "batch")
    
    try:
        conn = BaseHook.get_connection('postgres_default')
        extra = json.loads(conn.extra) if conn.extra else {}
        pg_host = conn.host
        pg_port = conn.port or '5432'
        pg_db = conn.schema or 'nyc_taxi_db'
        pg_user = conn.login
        pg_password = conn.password
        pg_schema = extra.get('schema', 'staging')
        pg_table = extra.get('table', 'stg_tripdata')
        pg_log_table = extra.get('log_table', 'stg_loadlog')
        jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"
        logger.info(f"Using Airflow Connection for PostgreSQL: {pg_host}:{pg_port}/{pg_db}")
    except Exception as e:
        logger.warning(f"Failed to get PostgreSQL Airflow Connection: {str(e)}. Falling back to environment variables.")
        pg_host = os.getenv("POSTGRES_HOST")
        pg_port = os.getenv("POSTGRES_PORT")
        pg_db = os.getenv("POSTGRES_DB")
        pg_user = os.getenv("POSTGRES_USER")
        pg_password = os.getenv("POSTGRES_PASSWORD")
        pg_schema = os.getenv("POSTGRES_SCHEMA_STAGING")
        pg_table = os.getenv("POSTGRES_STAGING_TABLE")
        pg_log_table = os.getenv("POSTGRES_STAGING_LOG")
        jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"

    spark = init_spark(endpoint, access_key, secret_key)
    minio_client = MinIOClient(endpoint, access_key, secret_key)

    try:
        zone_df = load_zone_lookup(spark).cache()
        parquet_files = minio_client.list_parquet_files(bucket_processed, prefix=folder_name + "/")
        
        if not parquet_files:
            logger.info("No files found in processed bucket.")
            spark.stop()
            return {"status": "no_files"}

        logger.info(f"Found {len(parquet_files)} parquet files to load.")
        processed_files = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"{pg_schema}.{pg_log_table}") \
            .option("user", pg_user) \
            .option("password", pg_password) \
            .option("driver", "org.postgresql.Driver") \
            .load() \
            .select("file_name") \
            .rdd.flatMap(lambda x: x).collect()
        
        unprocessed_files = [f for f in parquet_files if os.path.basename(f) not in processed_files]

        if not unprocessed_files:
            logger.info("No new files to process.")
            zone_df.unpersist(blocking=True)
            return {"status": "no_new_files"}

        total_rows = 0
        logger.info(f"Processing {len(unprocessed_files)} new parquet files.")
        for object_name in unprocessed_files:
            file_name = os.path.basename(object_name)
            parquet_path = f"s3a://{bucket_processed}/{object_name}"
            logger.info(f"Processing file: {file_name}")

            try:
                start_time = time.time()
                df = spark.read.parquet(parquet_path)
                count_before = df.count()
                
                df = enrich_with_zone_and_daypart(df, zone_df)
                df = apply_business_filters(df)
                df = apply_iqr_filter(df)
                df = deduplicate(df)
                df = add_trip_key(df)
                
                count_after = df.count()
                total_rows += count_after
                logger.info(f"Rows after processing: {count_after:,} (from {count_before:,})")
                
                insert_to_postgres(spark, df, file_name, jdbc_url, pg_schema, pg_table, pg_log_table, pg_user, pg_password)
                logger.info(f"Inserted {file_name} to staging in {time.time() - start_time:.2f} seconds")
                
                # Memory cleanup
                del df
                gc.collect()
                
            except Exception as e:
                logger.error(f"Failed to process {file_name}: {str(e)}")
                raise AirflowException(f"Processing failed for {file_name}: {str(e)}")
        
        zone_df.unpersist(blocking=True)
        logger.info("All files processed successfully.")

        #Push total rows to XCom
        context['ti'].xcom_push(key='total_rows', value=total_rows)
        return {"status": "success", "total_row": total_rows}
    
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise AirflowException(f"Pipeline failed: {str(e)}")
    
    finally:
        spark.stop()

if __name__ == "__main__":
    processed_to_staging()
