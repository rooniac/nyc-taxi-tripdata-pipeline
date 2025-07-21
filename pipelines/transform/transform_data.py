import os
import sys
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from pathlib import Path
from typing import Dict
from airflow.hooks.base import BaseHook
import s3fs
import json
import logging


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(ROOT_DIR)

from utils.minio_utils import MinIOClient
from utils.helpers import load_cfg, get_config_value


CFG_FILE = os.path.join(ROOT_DIR, "configs", "datalake.yaml")
LOOKUP_PATH = os.path.join(ROOT_DIR, "pipelines", "data", "taxi_zone_lookup.csv")
YEARS = ["2023", "2024", "2025"]
STREAMING_PATHS = [
    "streaming/2025/yellow_tripdata/",
    "streaming/2025/green_tripdata/"
]

logging.basicConfig(
    level=logging.info,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================
# Data Transform Functions
# =============================

PROCESSED_SCHEMA = {
    # "pickup_datetime": "datetime64[ns]",
    # "dropoff_datetime": "datetime64[ns]", 
    "passenger_count": np.float64,
    "vendorid": np.int64,
    "payment_type": np.int64,
    "pulocationid": np.int64,
    "dolocationid": np.int64,
    "service_type": np.int64,
    "ratecodeid": np.float64,
    "trip_distance": np.float64,
    "fare_amount": np.float64,
    "extra": np.float64,
    "mta_tax": np.float64,
    "tip_amount": np.float64,
    "tolls_amount": np.float64,
    "total_amount": np.float64,
    "improvement_surcharge": np.float64,
    "congestion_surcharge": np.float64,
}


def drop_column(df, file):
    for col in ["store_and_fwd_flag", "cbd_congestion_fee"]:
        if col in df.columns:
            df = df.drop(columns=[col])
            print(f"Dropped '{col} from {file}'")
    return df


def merge_taxi_zone(df, file):
    df_lookup = pd.read_csv(LOOKUP_PATH)

    def merge_and_rename(df, location_id, lat_col, long_col):
        df = df.merge(df_lookup, left_on=location_id, right_on="LocationID", how="left")
        df = df.drop(columns=["LocationID", "Borough", "service_zone", "zone"])
        df = df.rename(columns={"latitude": lat_col, "longitude": long_col})
        return df

    if "pickup_latitude" not in df.columns and "pulocationid" in df.columns:
        df = merge_and_rename(df, "pulocationid", "pickup_latitude", "pickup_longitude")

    if "dropoff_latitude" not in df.columns and "dolocationid" in df.columns:
        df = merge_and_rename(df, "dolocationid", "dropoff_latitude", "dropoff_longitude")

    # Xóa các cột rác
    df = df.drop(columns=[col for col in df.columns if "Unnamed" in col], errors='ignore')

    # Kiểm tra đầy đủ cột lat/long sau khi merge
    required_cols = ["pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude"]
    for col_name in required_cols:
        if col_name not in df.columns:
            raise ValueError(f"[{file}] Missing column after merge: {col_name}")

    # Drop dòng thiếu tọa độ
    df = df.dropna(subset=required_cols)

    return df


def ensure_service_type(df, file):
    df.columns = map(str.lower, df.columns)
    if "service_type" not in df.columns:
        if file.startswith("yellow"):
            df["service_type"] = 1
        elif file.startswith("green"):
            df["service_type"] = 2
        else:
            raise ValueError(f"Cannot infer service type from file name: {file}")
        print(f"Add 'service_type' to {file}")

    return df

def process(df, file):
    service_type = df["service_type"].iloc[0]

    if service_type == 2: # green
        df.rename(columns={
            "lpep_pickup_datetime": "pickup_datetime",
            "lpep_dropoff_datetime": "dropoff_datetime",
            "ehail_fee": "fee"
        }, inplace=True)
        if "trip_type" in df.columns:
            df.drop(columns=["trip_type"], inplace=True)
    elif service_type == 1: # yellow
        df.rename(columns={
            "tpep_pickup_datetime": "pickup_datetime",
            "tpep_dropoff_datetime": "dropoff_datetime",
            "airport_fee": "fee"
        }, inplace=True)

    # Gán default payment_type = 5 nếu null (5 = unknown)
    if "payment_type" in df.columns:
        df["payment_type"] = df["payment_type"].fillna(5).astype(int)
    for col_name in ["dolocationid", "pulocationid", "vendorid"]:
        if col_name in df.columns:
            df[col_name] = df[col_name].astype(int)


    if "fee" in df.columns:
        df.drop(columns=["fee"], inplace=True)
    
    df = df.dropna()

    # Ép kiểu cho datetime
    df.loc[:, "pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df.loc[:, "dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

    # Ép kiểu schema cho processed bucket
    for col, dtype in PROCESSED_SCHEMA.items():
        if col in df.columns:
            try:
                df.loc[:, col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)
            except Exception as e:
                print(f"[{file}] Failed to cast column '{col}' to {dtype}: {e}")

    df = df.reindex(sorted(df.columns), axis=1)
    
    print(f"Processed data from file: {file}")
    return df

# =============================
# Transform Function for Airflow
# =============================

def transform_data_for_airflow(**context) -> Dict[str, str]:
    try:
        conn = BaseHook.get_connection('minio_default')
        extra = json.loads(conn.extra) if conn.extra else {}
        endpoint = conn.host.replace('http://', '') if conn.host else extra.get('endpoint_url', '').replace('http://', '')
        if not endpoint:
            raise ValueError("MinIO endpoint is not configured in Airflow Connection")
        access_key = conn.login or "minioadmin"
        secret_key = conn.password or "minioadmin123"
        secure = extra.get('use_ssl', False)
        bucket_raw = extra.get('bucket_raw', 'raw')
        bucket_processed = extra.get('bucket_processed', 'processed')
        logger.info(f"Using Airflow Connection for MinIO: {endpoint}")
    except Exception as e:
        logger.warning(f"Failed to get Airflow Connection: {str(e)}. Falling back to datalake.yaml")
        cfg = load_cfg(CFG_FILE)
        endpoint = get_config_value(cfg, ["datalake", "endpoint"], default="minio:9000")
        access_key = get_config_value(cfg, ["datalake", "access_key"], default="minioadmin")
        secret_key = get_config_value(cfg, ["datalake", "secret_key"], default="minioadmin123")
        bucket_raw = get_config_value(cfg, ["datalake", "buckets", "raw"], default="raw")
        bucket_processed = get_config_value(cfg, ["datalake", "buckets", "processed"], default="processed")
        secure = False
    
    minio_client = MinIOClient(endpoint, access_key, secret_key, secure)
    minio_client.create_bucket(bucket_processed)

    processed_paths = {}
    temp_dir = Path('/opt/airflow/data/temp')
    temp_dir.mkdir(parents=True, exist_ok=True)

    # prefixes = [f"{year}/" for year in YEARS] + STREAMING_PATHS
    prefixes = ["2025/"]
    for prefix in prefixes:
        parquet_files = minio_client.list_parquet_files(bucket_raw, prefix=prefix)
        for object_name in parquet_files:
            file_name = os.path.basename(object_name)
            processed_minio_path = f"batch/{file_name}"
            local_temp_path = temp_dir / file_name

            # Kiểm tra file đã tồn tại trong bucket processed
            existing_files = minio_client.list_objects(bucket_processed, prefix="batch/", recursive=True)
            existing_paths = [obj.object_name for obj in existing_files]

            if processed_minio_path in existing_paths:
                logger.info(f"File already exists in MinIO bucket {bucket_processed}, skipping: {processed_minio_path}")
                continue
            
            # Tải file từ bucket raw
            logger.info(f"Downloading {object_name} from MinIO bucket {bucket_raw}...")
            minio_client.fget_object(bucket_raw, object_name, str(local_temp_path))

            # Same logic but require more RAM to compile
            
            # # Transform
            # logger.info(f"Transforming {file_name}...")
            # df = pd.read_parquet(local_temp_path, engine="pyarrow")
            # logger.info(f"[{file_name}] Original row count: {len(df)}")
            # df = drop_column(df, file_name)
            # df = ensure_service_type(df, file_name)
            # df = merge_taxi_zone(df, file_name)
            # df = process(df, file_name)
            # logger.info(f"[{file_name}] Final row count: {len(df)}")

            # df.to_parquet(
            #     local_temp_path,
            #     index=False,
            #     engine="pyarrow",
            #     coerce_timestamps="ms",
            #     flavor="spark"
            # )

            logger.info(f"Transforming {file_name}...")
            parquet_file = pq.ParquetFile(local_temp_path)
            transformed_dfs = []
            for batch in parquet_file.iter_batches(batch_size=100000): #Batch size 100k rows
                df = batch.to_pandas()
                logger.info(f"[{file_name}] Processing batch with {len(df)} rows")
                df = drop_column(df, file_name)
                df = ensure_service_type(df, file_name)
                df = merge_taxi_zone(df, file_name)
                df = process(df, file_name)
                transformed_dfs.append(df)
            
            df = pd.concat(transformed_dfs, ignore_index=True)
            logger.info(f"[{file_name}] Final row count: {len(df)}")

            df.to_parquet(
                local_temp_path,
                index=False,
                engine="pyarrow",
                coerce_timestamps="ms",
                flavor='spark'
            )
            logger.info(f"Saved transformed file to {local_temp_path}")

            # Upload to bucket processed
            logger.info(f"Uploading {file_name} to MinIO bucket {bucket_processed}: {processed_minio_path}")
            minio_client.fput_object(bucket_processed, processed_minio_path, str(local_temp_path))
            logger.info(f"Successfully uploaded {file_name} to {processed_minio_path}")
            processed_paths[file_name] = str(local_temp_path)

            # Xóa file tạm
            if local_temp_path.exists():
                local_temp_path.unlink()
                logger.info(f"Deleted temporary file: {local_temp_path}")

        return processed_paths




# =============================
# Main Transform Function
# =============================

def transform_data():
    cfg = load_cfg(CFG_FILE)
    if cfg is None:
        print("Failed to load configuration. Exiting.")
        return

    endpoint = get_config_value(cfg, ["datalake", "endpoint"])
    access_key = get_config_value(cfg, ["datalake", "access_key"])
    secret_key = get_config_value(cfg, ["datalake", "secret_key"])

    BUCKET_RAW = get_config_value(cfg, ["datalake", "buckets", "raw"], "raw")
    BUCKET_PROCESSED = get_config_value(cfg, ["datalake", "buckets", "processed"], "processed")
    FOLDER_PROCESSED = get_config_value(cfg, ["datalake", "folder_name"], "batch")

    # Create client
    minio_client = MinIOClient(endpoint, access_key, secret_key)
    minio_client.create_bucket(BUCKET_PROCESSED)

    s3_fs = s3fs.S3FileSystem(
        anon=False,
        key=access_key,
        secret=secret_key,
        client_kwargs={'endpoint_url': f"http://{endpoint}"}
    )

    def process_files(prefix):
        parquet_files = minio_client.list_parquet_files(BUCKET_RAW, prefix=prefix)

        for object_name in parquet_files:
            file_name = os.path.basename(object_name)
            print(f"\nReading raw file from MinIO: {object_name}")

            with s3_fs.open(f"s3://{BUCKET_RAW}/{object_name}", mode="rb") as f:
                df = pd.read_parquet(f, engine="pyarrow")
            
            print(f"[{file_name}] Original row count: {len(df)}")  

            df = drop_column(df, file_name)
            df = ensure_service_type(df, file_name)
            df = merge_taxi_zone(df, file_name)
            df = process(df, file_name)

            print(f"[{file_name}] Final row count: {len(df)}")

            target_path = f"s3://{BUCKET_PROCESSED}/{FOLDER_PROCESSED}/{file_name}"
            df.to_parquet(
                target_path,
                index=False,
                filesystem=s3_fs,
                engine="pyarrow",
                coerce_timestamps="ms",
                flavor="spark"
            )

            print(f"Save processed file to MinIO: {target_path}")
            print("=" * 80)
            
    # Process ingest data
    for year in YEARS:
        process_files(f"{year}/")
    # Process streaming data
    for path in STREAMING_PATHS:
        process_files(path)

    

if __name__ == "__main__":
    transform_data()
