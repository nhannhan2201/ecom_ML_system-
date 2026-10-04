"""
================================================================================
DAG: DP2_BRONZE_TO_SILVER_AND_GOLD - DATA PROCESSING & STAR SCHEMA DWH (RUBRIC 4.0Đ)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
Tác giả: Hoàng Minh Nhân & Antigravity AI

MỤC TIÊU RUBRIC AIRFLOW ORCHESTRATION:
1. Centralized Connections & Variables (Admin -> Variables & Connections):
   - python_exec_path: Container Python thực thi (mặc định sys.executable) có sẵn PySpark 3.5.0 & Delta Lake 3.0.0
   - project_root: Đường dẫn root dự án (/opt/airflow/ecom_project)
   - minio_endpoint: Endpoint MinIO (http://ecom_minio:9000)
   - raw_base_path: s3a://ecommerce-raw/batch
   - lakehouse_bucket_path: s3a://ecommerce-lakehouse
   - Connections: postgres_dwh, minio_s3_conn
2. Ingest Stage (2.0đ):
   - Đọc Bronze Zone -> Khử trùng lặp 2.05% rác (Window Top-1) -> Silver stg_events.
   - Xử lý Data Skew bằng AQE Skew Join + Broadcast Hash Join + Salting key demo.
   - Xây dựng Gold DWH (Kimball Star Schema): dim_product (SCD Type 2), dim_user, fact_user_events (Pure Fact).
   - Lakehouse Storage Optimization: OPTIMIZE & ZORDER BY (user_id) + VACUUM dọn dẹp file phân mảnh.
3. Validate Stage (2.0đ):
   - Kiểm tra Data Quality & Referential Integrity:
     * Silver clean rows > 0, Fact rows == Silver clean rows.
     * Fact có product_sk hợp lệ (0 NULL foreign keys).
     * Pure Fact Contract: Không chứa product_id (đã chuẩn hóa surrogate key).
4. Tự động Trigger DP3 Pipeline sau khi Validate thành công.
================================================================================
"""

import os
import sys
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Thiết lập múi giờ Việt Nam (Asia/Ho_Chi_Minh)
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

from airflow.hooks.base import BaseHook

# Lấy biến cấu hình tập trung từ Airflow Variables
PYTHON_EXEC = Variable.get("python_exec_path", default_var=sys.executable)
PROJECT_ROOT = Variable.get("project_root", default_var="/opt/airflow/ecom_project")
RAW_BASE_PATH = Variable.get("raw_base_path", default_var="s3a://ecommerce-raw/batch")
LAKEHOUSE_PATH = Variable.get("lakehouse_bucket_path", default_var="s3a://ecommerce-lakehouse")

# Lấy cấu hình kết nối MinIO tập trung từ Airflow Connection 'minio_s3_conn'
def _get_minio_connection():
    try:
        conn = BaseHook.get_connection("minio_s3_conn")
        endpoint = conn.extra_dejson.get("endpoint_url") or conn.host or "http://ecom_minio:9000"
        access_key = conn.login or ""
        secret_key = conn.password or ""
        return endpoint, access_key, secret_key
    except Exception:
        endpoint = Variable.get("minio_endpoint", default_var="http://ecom_minio:9000")
        access_key = Variable.get("aws_access_key_id", default_var=os.environ.get("AWS_ACCESS_KEY_ID", ""))
        secret_key = Variable.get("aws_secret_access_key", default_var=os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        return endpoint, access_key, secret_key

# Lấy cấu hình kết nối PostgreSQL DWH tập trung từ Airflow Connection 'postgres_dwh'
def _get_postgres_connection():
    try:
        conn = BaseHook.get_connection("postgres_dwh")
        host = conn.host or os.getenv("POSTGRES_HOST", "ecom_postgres")
        port = str(conn.port or os.getenv("POSTGRES_PORT", "5432"))
        db = conn.schema or os.getenv("POSTGRES_DB", "ecom_dwh")
        user = conn.login or os.getenv("POSTGRES_USER", "")
        password = conn.password or os.getenv("POSTGRES_PASSWORD", "")
        return host, port, db, user, password
    except Exception:
        return (
            os.getenv("POSTGRES_HOST", "ecom_postgres"),
            os.getenv("POSTGRES_PORT", "5432"),
            os.getenv("POSTGRES_DB", "ecom_dwh"),
            os.getenv("POSTGRES_USER", ""),
            os.getenv("POSTGRES_PASSWORD", "")
        )

MINIO_ENDPOINT, AWS_ACCESS_KEY, AWS_SECRET_KEY = _get_minio_connection()
PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD = _get_postgres_connection()

# Thiết lập môi trường thực thi đầy đủ cho Spark & Delta Lake bên trong Airflow
CONTAINER_JAVA_HOME = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")
JAVA_HOME_PATH = Variable.get("java_home", default_var=CONTAINER_JAVA_HOME)
task_env = {
    **os.environ,
    "PYTHONPATH": f"{PROJECT_ROOT}:{PROJECT_ROOT}/src",
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "POSTGRES_HOST": PG_HOST,
    "POSTGRES_PORT": PG_PORT,
    "POSTGRES_DB": PG_DB,
    "POSTGRES_USER": PG_USER,
    "POSTGRES_PASSWORD": PG_PASSWORD,
    "JAVA_HOME": JAVA_HOME_PATH,
    "PATH": f"{JAVA_HOME_PATH}/bin:{os.environ.get('PATH', '')}",
}

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dp2_bronze_to_silver_and_gold",
    default_args=default_args,
    description="[DP2] Pipeline to Ingest Bronze into Silver & Gold Zone (Deduplication, AQE Skew Join, Star Schema DWH & Z-Order)",
    schedule_interval=None,  # Chạy theo Trigger từ DP1 hoặc Manual Run
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=False,
    tags=["lakehouse", "silver", "gold", "dwh", "dp2", "ingest", "validate", "spark"],
) as dag:

    # --------------------------------------------------------------------------
    # 1. INGEST STAGE (RUBRIC: 2.0Đ)
    # --------------------------------------------------------------------------
    ingest_stage = BashOperator(
        task_id="ingest_stage",
        bash_command=(
            f"{PYTHON_EXEC} {PROJECT_ROOT}/src/spark/spark_optimized.py "
            f"--stage dp2 --step ingest --no-wait "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--raw-base-path {RAW_BASE_PATH} "
            f"--lakehouse-path {LAKEHOUSE_PATH}"
        ),
        env=task_env,
        execution_timeout=timedelta(minutes=20),
    )

    # --------------------------------------------------------------------------
    # 2. VALIDATE STAGE (RUBRIC: 2.0Đ)
    # --------------------------------------------------------------------------
    validate_stage = BashOperator(
        task_id="validate_stage",
        bash_command=(
            f"{PYTHON_EXEC} {PROJECT_ROOT}/src/spark/spark_optimized.py "
            f"--stage dp2 --step validate --no-wait "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--raw-base-path {RAW_BASE_PATH} "
            f"--lakehouse-path {LAKEHOUSE_PATH}"
        ),
        env=task_env,
        execution_timeout=timedelta(minutes=10),
    )

    # --------------------------------------------------------------------------
    # 3. TRIGGER DOWNSTREAM PIPELINE (DP3)
    # --------------------------------------------------------------------------
    trigger_dp3_pipeline = TriggerDagRunOperator(
        task_id="trigger_dp3_pipeline",
        trigger_dag_id="dp3_compute_offline_features",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Thứ tự thực thi chuẩn xác theo Rubric: Ingest stage >> Validate stage >> Trigger DP3
    ingest_stage >> validate_stage >> trigger_dp3_pipeline
