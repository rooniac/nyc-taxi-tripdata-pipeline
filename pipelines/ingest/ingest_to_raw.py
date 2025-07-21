import logging
from pathlib import Path
from airflow.hooks.base import BaseHook
from minio import Minio
from utils.helpers import load_cfg, get_config_value
import json
from typing import Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load config for fallback
CONFIG_PATH = "/opt/airflow/configs/datalake.yaml"
cfg = load_cfg(CONFIG_PATH)
if not cfg:
    logger.warning("Failed to load config, using default values")
ENDPOINT = get_config_value(cfg, ["datalake", "endpoint"], default="minio:9000")
ACCESS_KEY = get_config_value(cfg, ["datalake", "access_key"], default="minioadmin")
SECRET_KEY = get_config_value(cfg, ["datalake", "secret_key"], default="minioadmin123")
BUCKET_NAME = get_config_value(cfg, ["datalake", "buckets", "raw"], default="raw")

def ingest_to_raw(file_paths: Dict[str, str], year: int, minio_client: Minio, bucket_name: str) -> None:
    if not bucket_name:
        logger.error("Bucket name is not provided")
        raise ValueError("Bucket name is required")

    final_dir = Path(f"/opt/airflow/data/{year}")
    final_dir.mkdir(parents=True, exist_ok=True)

    for data_type, file_path in file_paths.items():
        file_path = Path(file_path)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")

        file_name = file_path.name
        minio_path = f"{year}/{file_name}"
        final_path = final_dir / file_name

        existing_files = minio_client.list_objects(bucket_name, prefix=f"{year}/", recursive=True)
        existing_paths = [obj.object_name for obj in existing_files]
        if minio_path in existing_paths:
            logger.info(f"File already exists in MinIO, skipping upload: {minio_path}")
        else:
            logger.info(f"Uploading {file_name} to MinIO: {minio_path}...")
            minio_client.fput_object(bucket_name, minio_path, str(file_path))
            logger.info(f"Successfully uploaded {file_name} to {minio_path}")

        if final_path.exists():
            logger.info(f"File already exists in final destination, skipping move: {final_path}")
        else:
            file_path.rename(final_path)
            logger.info(f"Moved {file_name} to {final_path}")

        new_dir = file_path.parent
        if new_dir.exists() and not any(new_dir.iterdir()):
            new_dir.rmdir()
            logger.info(f"Deleted empty directory: {new_dir}")

def ingest_to_raw_for_airflow(**context) -> None:
    file_paths = context['task_instance'].xcom_pull(task_ids='download_parquet')
    if not file_paths:
        logger.warning("No file paths received from XCom, skipping ingestion")
        return

    logical_date = context['logical_date']
    year = logical_date.year

    try:
        conn = BaseHook.get_connection('minio_default')
        extra = json.loads(conn.extra) if conn.extra else {}
        endpoint = conn.host.replace('http://', '') if conn.host else extra.get('endpoint_url', '').replace('http://', '')
        if not endpoint:
            raise ValueError("MinIO endpoint is not configured in Airflow Connection")
        access_key = conn.login or ACCESS_KEY
        secret_key = conn.password or SECRET_KEY
        secure = extra.get('use_ssl', False)
        bucket_name = extra.get('bucket_name', BUCKET_NAME)
        logger.info(f"Using Airflow Connection for MinIO: {endpoint}")
    except Exception as e:
        logger.warning(f"Failed to get Airflow Connection: {str(e)}. Falling back to datalake.yaml")
        endpoint = ENDPOINT
        access_key = ACCESS_KEY
        secret_key = SECRET_KEY
        secure = False
        bucket_name = BUCKET_NAME

    minio_client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

    minio_client.bucket_exists(bucket_name) or minio_client.make_bucket(bucket_name)
    ingest_to_raw(file_paths, year, minio_client, bucket_name)