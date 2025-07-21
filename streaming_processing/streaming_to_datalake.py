import os
import sys
import io
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(ROOT_DIR)

from utils.helpers import get_config_value, load_cfg
from utils.minio_utils import MinIOClient

load_dotenv(dotenv_path=os.path.join(ROOT_DIR, ".env"))

# Load config
CFG_FILE = os.path.join(ROOT_DIR, "configs", "datalake.yaml")
cfg = load_cfg(CFG_FILE)

bucket_raw = get_config_value(cfg, ["datalake", "buckets", "raw"])
endpoint = get_config_value(cfg, ["datalake", "endpoint"])
access_key = get_config_value(cfg, ["datalake", "access_key"])
secret_key = get_config_value(cfg, ["datalake", "secret_key"])

# Define topic-table mapping
TOPIC_TABLE_MAP = {
    "nyc.cdc_source.yellow_tripdata": {
        "service_type": 1,
        "schema": StructType([
            StructField("vendorid", IntegerType(), True),
            StructField("tpep_pickup_datetime", LongType(), True),
            StructField("tpep_dropoff_datetime", LongType(), True),
            StructField("passenger_count", LongType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("ratecodeid", LongType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("pulocationid", IntegerType(), True),
            StructField("dolocationid", IntegerType(), True),
            StructField("payment_type", LongType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True),
            StructField("cbd_congestion_fee", DoubleType(), True),
        ])
    },
    "nyc.cdc_source.green_tripdata": {
        "service_type": 2,
        "schema": StructType([
            StructField("vendorid", IntegerType(), True),
            StructField("lpep_pickup_datetime", LongType(), True),
            StructField("lpep_dropoff_datetime", LongType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("ratecodeid", LongType(), True),
            StructField("pulocationid", IntegerType(), True),
            StructField("dolocationid", IntegerType(), True),
            StructField("passenger_count", LongType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("ehail_fee", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("payment_type", LongType(), True),
            StructField("trip_type", LongType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("cbd_congestion_fee", DoubleType(), True),
        ])
    }
}

def ensure_streaming_folder_exists():
    client = MinIOClient(endpoint, access_key, secret_key).create_conn()
    dummy_path = "streaming/2025/.init"
    if not any(obj.object_name.startswith("streaming/2025/") for obj in client.list_objects(bucket_raw, prefix="streaming/2025/", recursive=True)):
        client.put_object(
            bucket_raw,
            dummy_path,
            data=io.BytesIO(b"init"),
            length=4
        )
        print(f"Created dummy file at {bucket_raw}/{dummy_path}")
    else:
        print(f"Folder {bucket_raw}/streaming/2025/ already exists")

def process_stream(topic: str, service_type: int, schema: StructType, spark: SparkSession):
    print(f"Subscribing to topic: {topic}")

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_parsed = (
        df_raw
        .selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), schema).alias("data"))
        .select("data.*")
        .withColumn("service_type", lit(service_type))
    )

    if "lpep_pickup_datetime" in df_parsed.columns:
        df_parsed = df_parsed \
            .withColumn("lpep_pickup_datetime", timestamp_micros("lpep_pickup_datetime")) \
            .withColumn("lpep_dropoff_datetime", timestamp_micros("lpep_dropoff_datetime"))

    elif "tpep_pickup_datetime" in df_parsed.columns:
        df_parsed = df_parsed \
            .withColumn("tpep_pickup_datetime", timestamp_micros("tpep_pickup_datetime")) \
            .withColumn("tpep_dropoff_datetime", timestamp_micros("tpep_dropoff_datetime"))



    expected_cols = set([field.name for field in schema.fields] + ["service_type"])
    df_cleaned = df_parsed.select([col(c) for c in df_parsed.columns if c in expected_cols])

    folder_name = topic.split(".")[-1]  # yellow_tripdata or green_tripdata
    output_path = f"s3a://{bucket_raw}/streaming/2025/{folder_name}"
    checkpoint_path = f"s3a://{bucket_raw}/streaming/checkpoints/{topic.replace('.', '_')}_checkpoint"


    query = (
        df_cleaned.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime="10 seconds")
        .queryName(topic.replace('.', '_'))
        .start()
    )

    return query

def main():
    ensure_streaming_folder_exists()

    jars_dir = os.path.join(ROOT_DIR, "jars")
    jars_list = [
        "spark-sql-kafka-0-10_2.12-3.5.0.jar",
        "kafka-clients-3.4.0.jar",
        "commons-pool2-2.11.1.jar",
        "spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    ]
    jars_paths = [os.path.join(jars_dir, jar) for jar in jars_list]

    spark = (
        SparkSession.builder.appName("StreamingToDatalake")
        .config("spark.jars", ",".join(jars_paths))
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{endpoint}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "10s")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # Create stream for each topic
    queries = []
    for topic, meta in TOPIC_TABLE_MAP.items():
        queries.append(process_stream(topic, meta["service_type"], meta["schema"], spark))

    
    last_batch_ids = {q.name: -1 for q in queries}
    last_active_time = time.time()
    idle_timeout = 30 #seconds

    print("Waiting for queries to initialize and process...")

    while True:
        for q in queries:
            if q.isActive:
                progress = q.lastProgress
                if progress and progress.get("batchId") is not None:
                    batch_id = progress["batchId"]
                    if batch_id > last_batch_ids[q.name]:
                        print(f"[{q.name}] Last batchId: {batch_id}")
                        last_batch_ids[q.name] = batch_id
                        last_active_time = time.time()

        if time.time() - last_active_time > idle_timeout:
            print("No new data processed in the last 60s. Stopping all queries.")
            for q in queries:
                q.stop()
            break

        time.sleep(5)

if __name__ == "__main__":
    main()
