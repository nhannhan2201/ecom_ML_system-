"""
Initialization script for Airflow Connections and Variables.
Ensures that all operational service connections (MinIO, PostgreSQL, Spark, Redis)
and shared variables are registered inside Airflow metadata database.
"""

import os
import sys
import json
from airflow import settings
from airflow.models import Connection, Variable


def init_connections():
    session = settings.Session()
    print("=" * 70)
    print("🚀 Initializing Centralized Airflow Connections and Variables...")
    print("   (Local Coursework Development Environment)")
    print("=" * 70)

    # Local development credentials (source of truth from environment or dev defaults)
    minio_access_key = os.getenv("MINIO_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    minio_endpoint = os.getenv("MINIO_INTERNAL_ENDPOINT") or os.getenv("MINIO_ENDPOINT", "http://ecom_minio:9000")

    pg_user = os.getenv("POSTGRES_DWH_USER") or os.getenv("POSTGRES_USER", "postgres")
    pg_password = os.getenv("POSTGRES_DWH_PASSWORD") or os.getenv("POSTGRES_PASSWORD", "postgres")
    pg_host = os.getenv("POSTGRES_DWH_HOST", "ecom_postgres")
    pg_port = int(os.getenv("POSTGRES_DWH_PORT", "5432"))
    pg_db = os.getenv("POSTGRES_DWH_DB") or os.getenv("POSTGRES_DB", "ecom_dwh")

    redis_host = os.getenv("REDIS_HOST", "ecom_redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))

    connections = [
        Connection(
            conn_id="minio_s3_conn",
            conn_type="aws",
            login=minio_access_key,
            password=minio_secret_key,
            extra=json.dumps({"endpoint_url": minio_endpoint}),
            description="MinIO S3 Object Storage for Lakehouse and Raw Buckets (Dev Creds)"
        ),
        Connection(
            conn_id="postgres_dwh",
            conn_type="postgres",
            host=pg_host,
            port=pg_port,
            schema=pg_db,
            login=pg_user,
            password=pg_password,
            description="PostgreSQL Data Warehouse for Analytical Star Schema"
        ),
        Connection(
            conn_id="spark_default",
            conn_type="generic",
            host="local[*]",
            port=4040,
            description="Local Spark Driver / Master for Lakehouse Processing"
        ),
        Connection(
            conn_id="redis_default",
            conn_type="redis",
            host=redis_host,
            port=redis_port,
            description="Redis Online Store for Feast Feature Serving"
        ),
    ]

    for conn in connections:
        existing = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
        if existing:
            existing.conn_type = conn.conn_type
            existing.host = conn.host
            existing.port = conn.port
            existing.schema = conn.schema
            existing.login = conn.login
            existing.password = conn.password
            existing.extra = conn.extra
            existing.description = conn.description
            print(f"  • Updated existing connection: {conn.conn_id}")
        else:
            session.add(conn)
            print(f"  • Created new connection: {conn.conn_id}")

    # Centralized pipeline variables (paths and parameters, non-secret)
    variables = {
        "project_root": "/opt/airflow/ecom_project",
        "raw_base_path": "s3a://ecommerce-raw/batch",
        "lakehouse_bucket_path": "s3a://ecommerce-lakehouse",
        "prediction_date": "2019-10-26",
        "minio_endpoint": minio_endpoint,
        "redis_host": redis_host,
        "redis_port": str(redis_port),
    }

    for key, val in variables.items():
        Variable.set(key, val)
        print(f"  • Set Variable: {key} = {val}")

    session.commit()
    session.close()
    print("=" * 70)
    print("✅ All Airflow Connections and Variables successfully initialized!")
    print("=" * 70)


if __name__ == "__main__":
    init_connections()
