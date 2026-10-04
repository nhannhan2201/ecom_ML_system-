"""Initialize Required MinIO S3 Buckets.

Creates ecommerce-raw, ecommerce-lakehouse, and ecommerce-feature-store buckets
if they do not already exist on MinIO.
"""

import os
import boto3
from dotenv import load_dotenv

load_dotenv()

REQUIRED_BUCKETS = [
    "ecommerce-raw",
    "ecommerce-lakehouse",
    "ecommerce-feature-store",
]


def init_buckets():
    endpoint_url = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID") or "minioadmin"
    secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or "minioadmin"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )

    print(f"Connecting to MinIO at {endpoint_url} to ensure buckets exist...")
    for bucket in REQUIRED_BUCKETS:
        try:
            s3.head_bucket(Bucket=bucket)
            print(f" - Bucket '{bucket}': OK (already exists)")
        except Exception:
            try:
                s3.create_bucket(Bucket=bucket)
                print(f" - Bucket '{bucket}': CREATED")
            except Exception as e:
                print(f" - Bucket '{bucket}': FAILED ({e})")
                raise


if __name__ == "__main__":
    init_buckets()
