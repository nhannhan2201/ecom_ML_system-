"""
================================================================================
DAG: DP1_RAW_TO_BRONZE - DATA LAKEHOUSE INGESTION & VALIDATION (RUBRIC 4.0Đ)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
Tác giả: Hoàng Minh Nhân & Antigravity AI

MỤC TIÊU RUBRIC AIRFLOW ORCHESTRATION:
1. Centralized Connections & Variables (Admin -> Variables & Connections):
   - python_exec_path: Container Python thực thi (mặc định sys.executable) có sẵn PySpark 3.5.0 & Delta Lake 3.0.0
   - project_root: Đường dẫn root dự án (/opt/airflow/ecom_project)
   - minio_endpoint: Endpoint MinIO (http://ecom_minio:9000)
   - raw_base_path: s3a://ecommerce-raw/batch
   - lakehouse_bucket_path: s3a://ecommerce-lakehouse
   - Connections: minio_s3_conn, spark_default
2. Ingest Stage (2.0đ):
   - Đọc Batch CSV cũ + mới (Schema Evolution qua mergeSchema=true) và Flink Stream Staging.
   - Nạp vào Delta Bronze (s3a://ecommerce-lakehouse/bronze/raw_events).
3. Validate Stage (2.0đ):
   - Kiểm tra Data Quality Contracts: 11/11 cột schema, 0 NULL user_id, row count > 0.
4. Tự động Trigger DP2 Pipeline sau khi Validate thành công.
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

MINIO_ENDPOINT, AWS_ACCESS_KEY, AWS_SECRET_KEY = _get_minio_connection()

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
    dag_id="dp1_raw_to_bronze",
    default_args=default_args,
    description="[DP1] Pipeline to Ingest Raw Data into Bronze Zone (Lakehouse Ingestion & Data Quality Validation)",
    schedule_interval=None,  # Chạy theo Trigger hoặc Manual Run
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=False,
    tags=["lakehouse", "bronze", "dp1", "ingest", "validate", "spark"],
) as dag:

    # --------------------------------------------------------------------------
    # 1. INGEST STAGE (RUBRIC: 2.0Đ)
    # --------------------------------------------------------------------------
    ingest_stage = BashOperator(
        task_id="ingest_stage",
        bash_command=(
            f"{PYTHON_EXEC} {PROJECT_ROOT}/src/spark/spark_optimized.py "
            f"--stage dp1 --step ingest --no-wait "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--raw-base-path {RAW_BASE_PATH} "
            f"--lakehouse-path {LAKEHOUSE_PATH}"
        ),
        env=task_env,
        execution_timeout=timedelta(minutes=15),
    )

    # --------------------------------------------------------------------------
    # 2. VALIDATE STAGE (RUBRIC: 2.0Đ)
    # --------------------------------------------------------------------------
    validate_stage = BashOperator(
        task_id="validate_stage",
        bash_command=(
            f"{PYTHON_EXEC} {PROJECT_ROOT}/src/spark/spark_optimized.py "
            f"--stage dp1 --step validate --no-wait "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--raw-base-path {RAW_BASE_PATH} "
            f"--lakehouse-path {LAKEHOUSE_PATH}"
        ),
        env=task_env,
        execution_timeout=timedelta(minutes=5),
    )

    # --------------------------------------------------------------------------
    # 3. TRIGGER DOWNSTREAM PIPELINE (DP2)
    # --------------------------------------------------------------------------
    trigger_dp2_pipeline = TriggerDagRunOperator(
        task_id="trigger_dp2_pipeline",
        trigger_dag_id="dp2_bronze_to_silver_and_gold",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    # Thứ tự thực thi chuẩn xác theo Rubric: Ingest stage >> Validate stage >> Trigger DP2
    ingest_stage >> validate_stage >> trigger_dp2_pipeline
