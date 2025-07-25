import os
from minio import Minio
import io
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

class MinIOClient:
    def __init__(self, endpoint=None, access_key=None, secret_key=None, secure=None):
        self.endpoint = endpoint or f"{os.getenv('MINIO_HOST', 'localhost')}:{os.getenv('MINIO_PORT', '9000')}"
        self.access_key = access_key or os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.secret_key = secret_key or os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        self.secure = secure or False

    def create_conn(self):
        client = Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )
        return client
    
    def create_bucket(self, bucket_name):
        if not bucket_name:
            raise ValueError("Bucket name is required")
        client = self.create_conn()
        if not client.bucket_exists(bucket_name=bucket_name):
            client.make_bucket(bucket_name=bucket_name)
            print(f"Bucket {bucket_name} created successfully!")
        else:
            print(f"Bucket {bucket_name} already exists, skip creating!")

    def list_parquet_files(self, bucket_name, prefix=""):
        if not bucket_name:
            raise ValueError("Bucket name is required")
        client = self.create_conn()
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        parquet_files = [obj.object_name for obj in objects if obj.object_name.endswith('.parquet')]
        return parquet_files

    def list_objects(self, bucket_name, prefix="", recursive=False):
        if not bucket_name:
            raise ValueError("Bucket name is required")
        client = self.create_conn()
        return list(client.list_objects(bucket_name, prefix=prefix, recursive=recursive))

    def fput_object(self, bucket_name, object_name, file_path):
        if not bucket_name or not object_name or not file_path:
            raise ValueError("Bucket name, object name, and file path are required")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        client = self.create_conn()
        client.fput_object(bucket_name, object_name, file_path)
        print(f"Uploaded file {file_path} to {bucket_name}/{object_name}")

    def fget_object(self, bucket_name, object_name, file_path):
        if not bucket_name or not object_name or not file_path:
            raise ValueError("Bucket name, object name, and file path are required")
        client = self.create_conn()
        client.fget_object(bucket_name, object_name, file_path)
        print(f"Downloaded {bucket_name}/{object_name} to {file_path}")

    def upload_text(self, bucket_name, object_name, text):
        if not bucket_name:
            raise ValueError("Bucket name is required")
        client = self.create_conn()
        client.put_object(
            bucket_name,
            object_name,
            data=io.BytesIO(text.encode("utf-8")),
            length=len(text)
        )
        print(f"Uploaded dummy file: {object_name} to bucket {bucket_name}")