"""
================================================================================
DAG: QUICKSTART_TUTORIAL_PIPELINE
Dành cho: Học viên làm quen với luồng điều phối của Apache Airflow
Mục đích:
  1. Hiểu cách khai báo 1 DAG (Workflow).
  2. Hiểu cách tạo các Task (BashOperator, PythonOperator).
  3. Hiểu cách thiết lập thứ tự thực thi (Task Dependencies: A >> B >> C).
  4. Trực tiếp quan sát các trạng thái của Task trên Airflow Web UI.
================================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ==============================================================================
# 1. ĐỊNH NGHĨA CÁC THAM SỐ MẶC ĐỊNH (DEFAULT ARGS)
# ==============================================================================
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 1,                      # Tự động thử lại 1 lần nếu gặp lỗi
    "retry_delay": timedelta(minutes=1), # Thời gian chờ trước khi thử lại
    "email_on_failure": False,
}

# ==============================================================================
# 2. KHỞI TẠO ĐỐI TƯỢNG DAG (DIRECTED ACYCLIC GRAPH)
# ==============================================================================
dag = DAG(
    dag_id="quickstart_tutorial_pipeline",
    default_args=default_args,
    description="Pipeline mẫu giúp hiểu cơ chế hoạt động của Apache Airflow",
    schedule_interval=None,             # Không tự chạy theo lịch, chỉ chạy khi bấm nút Trigger
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["quickstart", "tutorial", "ecom_ml"],
)

# ==============================================================================
# 3. ĐỊNH NGHĨA CÁC HÀM XỬ LÝ PYTHON
# ==============================================================================
def extract_sample_data(**context):
    """Giả lập bước nạp dữ liệu (Data Ingestion Stage)"""
    print("⏳ [BƯỚC 1]: Đang kết nối tới nguồn dữ liệu và kéo dữ liệu mẫu...")
    sample_records = [
        {"user_id": 1001, "event": "view", "product": "iPhone 15", "price": 999.0},
        {"user_id": 1002, "event": "cart", "product": "MacBook Air", "price": 1199.0},
        {"user_id": 1003, "event": "purchase", "product": "AirPods Pro", "price": 249.0},
    ]
    print(f"✅ Đã kéo thành công {len(sample_records)} bản ghi giao dịch.")
    # Đẩy số lượng bản ghi vào XCom để task sau có thể đọc được
    return len(sample_records)


def validate_data_quality(**context):
    """Giả lập bước kiểm tra chất lượng dữ liệu (Data Validation Stage)"""
    print("🔍 [BƯỚC 2]: Đang thực hiện kiểm tra chất lượng dữ liệu (Data Quality)...")
    # Lấy kết quả từ task trước thông qua cơ chế XCom của Airflow
    ti = context["ti"]
    record_count = ti.xcom_pull(task_ids="task_2_extract_sample_data")
    
    print(f"📊 Nhận được {record_count} bản ghi từ bước Ingestion.")
    assert record_count > 0, "❌ Lỗi: Không có dữ liệu để xử lý!"
    print("✅ Kiểm tra Data Contract: Không có giá trị NULL, kiểu dữ liệu hợp lệ!")


# ==============================================================================
# 4. TẠO CÁC TASK CỤ THỂ BẰNG OPERATORS
# ==============================================================================

# Task 1: Khởi động pipeline (chạy lệnh shell Linux)
task_1_start = BashOperator(
    task_id="task_1_start_pipeline",
    bash_command='echo "🚀 [AIRFLOW QUICKSTART]: Bắt đầu thực thi Data Pipeline lúc $(date)"',
    dag=dag,
)

# Task 2: Nạp dữ liệu (gọi hàm Python)
task_2_extract = PythonOperator(
    task_id="task_2_extract_sample_data",
    python_callable=extract_sample_data,
    provide_context=True,
    dag=dag,
)

# Task 3: Xác thực chất lượng dữ liệu (gọi hàm Python)
task_3_validate = PythonOperator(
    task_id="task_3_validate_data_quality",
    python_callable=validate_data_quality,
    provide_context=True,
    dag=dag,
)

# Task 4: Thông báo hoàn tất thành công (chạy lệnh shell Linux)
task_4_complete = BashOperator(
    task_id="task_4_pipeline_success",
    bash_command='echo "🎉 [AIRFLOW QUICKSTART]: Toàn bộ pipeline đã hoàn tất thành công!"',
    dag=dag,
)

# ==============================================================================
# 5. THIẾT LẬP THỨ TỰ CHẠY (TASK DEPENDENCY CHAIN)
# ==============================================================================
# Toán tử >> quy định: Task trước chạy XONG và THÀNH CÔNG thì Task sau mới được chạy!
task_1_start >> task_2_extract >> task_3_validate >> task_4_complete
