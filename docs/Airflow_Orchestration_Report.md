# Báo Cáo Điều Phối Dữ Liệu Tự Động Airflow (Airflow Pipelines Report)
> Hạng mục rubric: Data Pipeline Orchestration: DP1 Ingest & Validate (4.0đ), DP2 Ingest & Validate (4.0đ), DP3 Ingest & Validate (4.0đ) = 12.0 điểm. Mã nguồn: [dags/](file:///home/nhan/Projects/ecom_ML_system/dags/). Cách chạy lại: `make init-airflow` và kích hoạt DAGs trên Airflow UI (`http://localhost:8080`).

## 1. Vấn đề cần giải quyết
Trong kiến trúc hồ và kho dữ liệu (Lakehouse & Data Warehouse), các bước trích xuất, biến đổi và nạp dữ liệu cần được thực thi theo thứ tự phụ thuộc nghiêm ngặt. Nếu thiếu điều phối tự động hoặc thiếu tầng kiểm định chất lượng độc lập, dữ liệu lỗi hoặc thiếu trường khóa sẽ tràn xuống các tầng hạ nguồn, làm sai lệch báo cáo kinh doanh và làm gián đoạn việc phục vụ đặc trưng cho mô hình học máy.

## 2. Cách làm
Dự án sử dụng Apache Airflow để điều phối 4 luồng xử lý tự động theo chuỗi liên tục (DP1 -> DP2 -> DP3 -> DP4):

### 2.1 Quản lý tập trung biến môi trường và kết nối
- Toàn bộ kết nối (`minio_s3_conn`, `postgres_dwh`, `redis_default`) và biến hệ thống (`bronze_bucket`, `feature_as_of_date`, `delta_vacuum_retain_hours`) được lưu trữ tập trung trên Airflow MetaStore, không ghi cứng thông tin định danh hay mật khẩu trong mã nguồn DAG.
- Các tác vụ Spark chạy qua `BashOperator` với cơ chế `append_env=True`, truyền an toàn thông tin kết nối từ Airflow Connection sang biến môi trường của tiến trình Spark.

### 2.2 Luồng xử lý DP1: Thô sang Bronze (`dp1_raw_to_bronze.py`)
- **Ingest stage**: Tác vụ `ingest_raw_batch_to_bronze` đọc các tệp CSV thô từ MinIO (`ecommerce-raw/batch/`) và dữ liệu staging của Flink, nạp vào bảng Bronze Delta Lake với cơ chế `mergeSchema=true`.
- **Validate stage**: Tác vụ `validate_bronze_contracts` kiểm tra số dòng đọc được và xác nhận không có giá trị trống (null) ở trường khóa `user_id`.
- **Trigger**: Sau khi kiểm định thành công, tự động kích hoạt pipeline DP2 qua `TriggerDagRunOperator`.

### 2.3 Luồng xử lý DP2: Bronze sang Silver và Gold (`dp2_bronze_to_silver_and_gold.py`)
- **Ingest stage**: Tác vụ `transform_bronze_to_silver_and_gold` thực hiện khử trùng lặp dữ liệu trên Bronze để tạo bảng Silver, sau đó xây dựng mô hình Star Schema trên PostgreSQL DWH gồm các bảng chiều `dim_product` (SCD Type 2), `dim_user` và bảng sự kiện `fact_user_events`.
- **Validate stage**: Tác vụ `validate_silver_and_gold` kiểm tra tính toàn vẹn của khóa chính và khóa ngoại surrogate `product_sk`.
- **Trigger**: Tự động kích hoạt pipeline DP3.

### 2.4 Luồng xử lý DP3: Tính toán đặc trưng và nhãn (`dp3_compute_offline_features.py`)
- **Ingest stage**: Tác vụ `compute_features_and_labels` tính toán bảng đặc trưng ngoại tuyến 30 ngày `feat_user_30d` và nhãn nhị phân `user_labels` (cửa sổ 1 giờ) theo biến ngày `feature_as_of_date`.
- **Validate stage**: Tác vụ `validate_feature_and_label_contracts` kiểm tra tính hợp lệ của hai cột thời gian bắt buộc cho Feature Store (`event_timestamp`, `created`) và giá trị nhãn chỉ thuộc tập [0, 1].
- **Trigger**: Tự động kích hoạt pipeline DP4 (`dp4_feast_materialize.py`) để đồng bộ dữ liệu đặc trưng sang Redis Online Store.

## 3. Kết quả đo
Kết quả chạy thực tế của các pipeline trên cụm Airflow:

| Pipeline DAG | Số lượng tác vụ | Thứ tự thực thi | Trạng thái |
| :--- | :--- | :--- | :--- |
| `dp1_raw_to_bronze` | 3 tasks | `ingest` -> `validate` -> `trigger_dp2` | Thành công |
| `dp2_bronze_to_silver_and_gold` | 3 tasks | `ingest` -> `validate` -> `trigger_dp3` | Thành công |
| `dp3_compute_offline_features` | 3 tasks | `compute` -> `validate` -> `trigger_dp4` | Thành công |
| `dp4_feast_materialize` | 2 tasks | `materialize_features` -> `verify_redis` | Thành công |

## 4. Minh chứng
![Minh chứng Airflow DP1 DAG](screenshots/E16_airflow_dp1_dag.png)
*Ảnh chứng minh: Đồ thị Graph/Grid View của DAG DP1 thể hiện tất cả các task hoàn thành thành công.*

![Minh chứng Airflow DP2 DAG](screenshots/E17_airflow_dp2_dag.png)
*Ảnh chứng minh: Đồ thị Graph/Grid View của DAG DP2 hoàn thành nạp Silver và Gold DWH.*

![Minh chứng Airflow DP3 DAG](screenshots/E18_airflow_dp3_dag.png)
*Ảnh chứng minh: Đồ thị Graph/Grid View của DAG DP3 hoàn tất tính toán đặc trưng và nhãn.*

![Minh chứng Airflow DP4 DAG](screenshots/E19_airflow_dp4_dag.png)
*Ảnh chứng minh: Đồ thị Graph/Grid View của DAG DP4 hoàn tất đồng bộ đặc trưng sang Redis.*

## 5. Hạn chế và lưu ý
1. Việc chia các tác vụ Spark thành các lệnh gọi `spark-submit` riêng biệt qua `BashOperator` giúp cách ly bộ nhớ giữa các công đoạn nhưng tốn thêm một khoảng thời gian khởi tạo JVM cho mỗi tác vụ.
2. Các DAG phụ thuộc nhau thông qua `TriggerDagRunOperator`, do đó khi cần chạy lại một bước độc lập, người vận hành có thể tắt cờ trigger hoặc kích hoạt trực tiếp DAG cần thiết từ giao diện web.
