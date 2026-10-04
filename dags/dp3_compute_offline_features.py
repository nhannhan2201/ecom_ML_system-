"""
================================================================================
DAG: DP3_COMPUTE_OFFLINE_FEATURES - FEATURE STORE TABLE & LABELS
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System

MỤC TIÊU VẬN HÀNH AIRFLOW ORCHESTRATION:
1. Centralized Connections & Variables (Admin -> Variables & Connections):
   - python_exec_path: Container Python thực thi có sẵn PySpark 3.5.0 & Delta Lake 3.0.0
   - project_root: Đường dẫn root dự án (/opt/airflow/ecom_project)
   - minio_endpoint: Endpoint MinIO (http://ecom_minio:9000)
   - lakehouse_bucket_path: s3a://ecommerce-lakehouse
   - prediction_date: Ngày dự đoán chốt chặn (2019-10-26)
   - Connections: spark_default, minio_s3_conn
2. Ingest Stage:
   - Đọc Silver Zone -> Tính toán 5 Offline Features cho bảng feat_user_30d:
     * f_views_30d, f_carts_30d, f_purchases_30d, f_spend_30d, f_distinct_categories_30d.
     * event_timestamp & created tuân thủ Data Contract của Feast Feature Store.
   - Tính toán Ground Truth user_labels (Sliding window 1m, lookahead 1h, chống Data Leakage).
   - Storage Optimization: OPTIMIZE & ZORDER BY (user_id) + VACUUM dọn dẹp file phân mảnh.
3. Validate Stage:
   - Kiểm tra Feast Schema Contract: bắt buộc tồn tại event_timestamp và created.
   - Kiểm tra Data Quality: feat_user_30d rows > 0, user_labels rows > 0.
   - Kiểm tra Target Distribution: nhãn target_purchase_1h thuộc tập nhị phân [0, 1].
4. Tự động Trigger DP4 Pipeline sau khi Validate thành công.
================================================================================
"""

import os
import sys
from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook

# Thiết lập múi giờ Việt Nam (Asia/Ho_Chi_Minh)
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# Lấy biến cấu hình tập trung từ Airflow Variables
PYTHON_EXEC = Variable.get("python_exec_path", default_var=sys.executable)
PROJECT_ROOT = Variable.get("project_root", default_var="/opt/airflow/ecom_project")
RAW_BASE_PATH = Variable.get("raw_base_path", default_var="s3a://ecommerce-raw/batch")
LAKEHOUSE_PATH = Variable.get("lakehouse_bucket_path", default_var="s3a://ecommerce-lakehouse")
PREDICTION_DATE = Variable.get("prediction_date", default_var="2019-10-26")

def _get_minio_connection():
    """Lấy cấu hình kết nối MinIO tập trung từ Airflow Connection 'minio_s3_conn'."""
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

CONTAINER_JAVA_HOME = os.environ.get("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64")
JAVA_HOME_PATH = Variable.get("java_home", default_var=CONTAINER_JAVA_HOME)
task_env = {
    **os.environ,
    "PYTHONPATH": f"{PROJECT_ROOT}:{PROJECT_ROOT}/src",
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": AWS_ACCESS_KEY,
    "MINIO_SECRET_KEY": AWS_SECRET_KEY,
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
    dag_id="dp3_compute_offline_features",
    default_args=default_args,
    description="[DP3] Pipeline to Compute Offline Feature Table (feat_user_30d) and Ground Truth Labels (Feast Contract & Z-Order)",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz=local_tz),
    catchup=False,
    tags=["lakehouse", "features", "feast", "dp3", "ingest", "validate", "spark"],
) as dag:

    # 1. INGEST STAGE
    ingest_stage = BashOperator(
        task_id="ingest_stage",
        bash_command=(
            f"{PYTHON_EXEC} {PROJECT_ROOT}/src/spark/spark_optimized.py "
            f"--stage dp3 --step ingest --no-wait "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--lakehouse-path {LAKEHOUSE_PATH} "
            f"--prediction-date {PREDICTION_DATE}"
        ),
        env=task_env,
        append_env=True,
        execution_timeout=timedelta(minutes=20),
    )

    # 2. VALIDATE STAGE
    validate_stage = BashOperator(
        task_id="validate_stage",
        bash_command=(
            f"{PYTHON_EXEC} {PROJECT_ROOT}/src/spark/spark_optimized.py "
            f"--stage dp3 --step validate --no-wait "
            f"--minio-endpoint {MINIO_ENDPOINT} "
            f"--lakehouse-path {LAKEHOUSE_PATH} "
            f"--prediction-date {PREDICTION_DATE}"
        ),
        env=task_env,
        append_env=True,
        execution_timeout=timedelta(minutes=10),
    )

    # 3. TRIGGER DOWNSTREAM PIPELINE (DP4)
    trigger_dp4_pipeline = TriggerDagRunOperator(
        task_id="trigger_dp4_pipeline",
        trigger_dag_id="dp4_feast_materialize",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    ingest_stage >> validate_stage >> trigger_dp4_pipeline
