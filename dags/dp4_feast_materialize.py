"""
================================================================================
DAG: DP4_FEAST_MATERIALIZE - FEATURE STORE ONLINE SYNC PIPELINE (RUBRIC 2.0Đ)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
Tác giả: Hoàng Minh Nhân & Antigravity AI
================================================================================
MỤC TIÊU RUBRIC AIRFLOW ORCHESTRATION & FEAST:
1. Build data pipeline trên Airflow để incremental materialize dữ liệu mới nhất
   từ Offline Lakehouse (MinIO Gold) qua Online Store (Redis).
2. Kiểm chứng các stages tuần tự chuẩn mực:
   - Stage 1 (task_validate_gold_source): Kiểm tra bảng MinIO Gold đã sẵn sàng.
   - Stage 2 (task_feast_incremental_materialize): Gọi script materialize.py --mode incremental để đồng bộ tăng dần lên Redis.
3. Kích hoạt chạy DAG trên Airflow Webserver (http://localhost:8080) và xác nhận trạng thái success (màu xanh lá cây).
================================================================================
"""

import os
import sys
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# Thiết lập múi giờ Việt Nam (Asia/Ho_Chi_Minh)
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

from airflow.hooks.base import BaseHook

# Lấy biến cấu hình tập trung từ Airflow Variables
PYTHON_EXEC = Variable.get("python_exec_path", default_var=sys.executable)
PROJECT_ROOT = Variable.get("project_root", default_var="/opt/airflow/ecom_project")

# Lấy cấu hình kết nối MinIO tập trung từ Airflow Connection 'minio_s3_conn'
def _get_minio_connection():
    try:
        conn = BaseHook.get_connection("minio_s3_conn")
        raw_ep = conn.extra_dejson.get("endpoint_url") or conn.host or "http://ecom_minio:9000"
        ep = raw_ep.replace("ecom_minio", "minio") if "ecom_minio" in raw_ep else raw_ep
        access_key = conn.login or ""
        secret_key = conn.password or ""
        return ep, access_key, secret_key
    except Exception:
        raw_ep = Variable.get("minio_endpoint", default_var="http://ecom_minio:9000")
        ep = raw_ep.replace("ecom_minio", "minio") if "ecom_minio" in raw_ep else raw_ep
        access_key = Variable.get("aws_access_key_id", default_var=os.environ.get("AWS_ACCESS_KEY_ID", ""))
        secret_key = Variable.get("aws_secret_access_key", default_var=os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
        return ep, access_key, secret_key

# Lấy cấu hình kết nối Redis tập trung từ Airflow Connection 'redis_default'
def _get_redis_connection():
    try:
        conn = BaseHook.get_connection("redis_default")
        host = conn.host or "ecom_redis"
        port = str(conn.port or 6379)
        return host, port
    except Exception:
        host = Variable.get("redis_host", default_var="ecom_redis")
        port = str(Variable.get("redis_port", default_var=6379))
        return host, port

MINIO_ENDPOINT, AWS_ACCESS_KEY, AWS_SECRET_KEY = _get_minio_connection()
REDIS_HOST, REDIS_PORT = _get_redis_connection()

# Thiết lập môi trường thực thi đầy đủ cho Feast & S3 MinIO bên trong Airflow
task_env = {
    **os.environ,
    "PYTHONNOUSERSITE": "1",
    "PYTHONPATH": f"{PROJECT_ROOT}:{PROJECT_ROOT}/feature_store",
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "REDIS_HOST": REDIS_HOST,
    "REDIS_PORT": REDIS_PORT,
    "REDIS_CONNECTION_STRING": f"{REDIS_HOST}:{REDIS_PORT}",
    "FEAST_USAGE": "False",
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "FEAST_S3_ENDPOINT_URL": MINIO_ENDPOINT,
    "S3_ENDPOINT_URL": MINIO_ENDPOINT,
}

default_args = {
    "owner": "mlops_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="dp4_feast_materialize",
    default_args=default_args,
    description="[DP4] Airflow Pipeline for Feast Incremental Materialization: Sync Gold Features to Redis Online Store",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=False,
    tags=["feature_store", "feast", "redis", "dp4", "materialize", "online_store"],
) as dag:

    # --------------------------------------------------------------------------
    # TASK 1: VALIDATE GOLD FEATURE SOURCE
    # --------------------------------------------------------------------------
    # Kiểm tra xem bảng Parquet trên MinIO Gold (feat_user_30d) đã tồn tại hay chưa
    task_validate_gold_source = BashOperator(
        task_id="task_validate_gold_source",
        bash_command=(
            f"{PYTHON_EXEC} -c \""
            f"import os, s3fs; "
            f"k = os.environ.get('AWS_ACCESS_KEY_ID', ''); "
            f"s = os.environ.get('AWS_SECRET_ACCESS_KEY', ''); "
            f"ep = os.environ.get('MINIO_ENDPOINT', '{MINIO_ENDPOINT}'); "
            f"assert k and s, 'MinIO AWS credentials must be configured in Airflow Connection minio_s3_conn'; "
            f"fs = s3fs.S3FileSystem(key=k, secret=s, client_kwargs={{'endpoint_url': ep}}); "
            f"assert fs.exists('ecommerce-lakehouse/gold/feat_user_30d'), 'MinIO Gold source not found!'; "
            f"print('✅ MinIO Gold Feature Source is READY!')\""
        ),
        env=task_env,
        execution_timeout=timedelta(minutes=5),
    )

    # --------------------------------------------------------------------------
    # TASK 2: FEAST INCREMENTAL MATERIALIZATION
    # --------------------------------------------------------------------------
    # Kích hoạt Feast nạp đồng bộ tăng dần đặc trưng 30 ngày từ MinIO lên RAM Redis
    task_feast_incremental_materialize = BashOperator(
        task_id="task_feast_incremental_materialize",
        bash_command=(
            f"cd {PROJECT_ROOT}/feature_store && "
            f"{PYTHON_EXEC} materialize.py --mode incremental"
        ),
        env=task_env,
        execution_timeout=timedelta(minutes=10),
    )

    # Thứ tự thực thi tuần tự chuẩn xác theo Rubric: Validate -> Materialize
    task_validate_gold_source >> task_feast_incremental_materialize
