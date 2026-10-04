# Cẩm Nang Vận Hành & Chạy Lại Từ Đầu (Runbook)

> Hướng dẫn toàn diện quy trình chạy lại hệ thống từ trạng thái sạch, thu thập số liệu thực nghiệm và chụp toàn bộ 30 ảnh minh chứng (`E01` - `E30`) theo danh mục tại `docs/EVIDENCE_TODO.md`.

---

## 1. Quy Trình Vận Hành 11 Bước

### Bước 1: Chuẩn bị môi trường và tệp cấu hình
1. Sao chép tệp biến môi trường từ mẫu:
   ```bash
   cp .env.example .env
   ```
2. Đảm bảo các gói JAR phụ thuộc đã sẵn sàng trong thư mục `docker/jars/` (Delta Lake 3.0.0, Hadoop-AWS 3.3.4, AWS Java SDK 1.12.262).
- **Kết quả mong đợi**: Tệp `.env` được tạo với các tham số kết nối mặc định; thư mục `docker/jars/` chứa đủ các tệp JAR cần thiết.
- **Mã ảnh chụp**: Không yêu cầu.

### Bước 2: Khởi động toàn bộ cụm dịch vụ (`make up-all`, `make check`)
1. Khởi động 7 cụm dịch vụ theo đúng thứ tự phụ thuộc:
   ```bash
   make up-all
   ```
2. Kiểm tra sức khỏe các cổng và tiến trình dịch vụ:
   ```bash
   make check
   ```
- **Kết quả mong đợi**: Toàn bộ 7 stack hiển thị trạng thái `[OK]` trên terminal.
- **Mã ảnh chụp**: Không yêu cầu.

### Bước 3: Khởi tạo kết nối và biến Airflow (`make init-airflow`)
Khởi tạo cấu trúc bảng cơ sở dữ liệu DWH và nạp các Connection (`minio_s3_conn`, `postgres_dwh`, `redis_default`) cùng Variables vào Airflow:
```bash
make init-airflow
```
- **Kết quả mong đợi**: Terminal in thông báo tạo schema DWH thành công và nạp đủ 4 connections vào Airflow MetaStore.
- **Mã ảnh chụp**: Không yêu cầu.

### Bước 4: Sinh dữ liệu mẫu & hồ sơ đặc tính (`make gen-data`)
1. Sinh dữ liệu mẻ chế độ small (10,200 dòng):
   ```bash
   make gen-data
   ```
2. Sinh dữ liệu lệch khóa để chuẩn bị cho thực nghiệm Spark:
   ```bash
   make gen-data-skewed
   ```
3. Tạo báo cáo hồ sơ dữ liệu:
   ```bash
   make profile-data
   ```
4. Khởi chạy luồng streaming ngắn để sinh manifest stream:
   ```bash
   make profile-stream
   ```
- **Kết quả mong đợi**: Dữ liệu tải lên MinIO bucket `ecommerce-raw/batch/`; tệp manifest và `docs/evidence/data_profile.md` được cập nhật.
- **Mã ảnh chụp cần chụp**:
  - `E01`: Terminal sau khi chạy `make gen-data` hiển thị bảng tổng kết dòng và dung lượng.
  - `E02`: MinIO Browser (`http://localhost:9001`) trong bucket `ecommerce-raw/batch/` thấy `raw_events_old.csv` và `raw_events_new.csv`.
  - `E03`: Terminal sau khi chạy `make profile-data` thể hiện tỉ lệ skew và duplicate.
  - `E04`: Terminal output test `pytest tests/test_generator.py -k test_schema_evolution` thấy 9 vs 10 cột.
  - `E05`: Terminal log sau khi chạy `make profile-stream` hiển thị tỉ lệ late arrival và duplicate stream.

### Bước 5: Thực nghiệm Spark Data Skew (`make spark-skew`)
Chạy benchmark so sánh 3 biến thể Sort-Merge Join (Baseline, AQE Skew Join, Salting):
```bash
make spark-skew
```
- **Kết quả mong đợi**: Kết quả đo 3 biến thể ghi vào `docs/evidence/spark_skew_experiment.md`.
- **Mã ảnh chụp cần chụp**:
  - `E07`: Spark UI (`http://localhost:4040/stages`) của Variant A thể hiện task straggler kéo dài.
  - `E08`: Spark UI của Variant B thể hiện AQE tự động chia nhỏ partition lệch.
  - `E09`: Spark UI của Variant C thể hiện các task phân bổ đều nhờ Salting.

### Bước 6: Chạy chuỗi Pipelines Airflow DP1 -> DP4
Kích hoạt tuần tự các DAG qua Airflow Web UI (`http://localhost:8080`) hoặc terminal:
```bash
docker exec airflow_scheduler airflow dags trigger dp1_raw_to_bronze
# Chờ DP1 hoàn tất, DP2 tự động kích hoạt
# Chờ DP2 hoàn tất, DP3 tự động kích hoạt
# Chờ DP3 hoàn tất, kích hoạt DP4
docker exec airflow_scheduler airflow dags trigger dp4_feast_materialize
```
- **Kết quả mong đợi**: Cả 4 DAGs đổi sang màu xanh lá (success) trên Airflow UI.
- **Mã ảnh chụp cần chụp**:
  - `E16`: Airflow UI DAG `dp1_raw_to_bronze` Graph View (tất cả tasks success).
  - `E17`: Airflow UI DAG `dp2_bronze_to_silver_and_gold` Graph View.
  - `E18`: Airflow UI DAG `dp3_compute_offline_features` Graph View.
  - `E19`: Airflow UI DAG `dp4_feast_materialize` Graph View.

### Bước 7: Thực nghiệm Flink Streaming (Baseline vs Optimized)
1. Chạy Flink Baseline để bộc lộ Backpressure:
   ```bash
   make flink-base
   ```
   Gửi lưu lượng burst và quan sát Flink Web UI (`http://localhost:8081`).
2. Dừng job baseline và nộp Flink Optimized:
   ```bash
   make flink-opt
   ```
- **Kết quả mong đợi**: Baseline bộc lộ Backpressure đỏ; Optimized chạy 3 slots, checkpoint đều đặn và 0 bản ghi trễ bị vứt bỏ.
- **Mã ảnh chụp cần chụp**:
  - `E11`: Flink UI Baseline với chỉ số Backpressure mức HIGH khi burst.
  - `E12`: Flink UI Optimized với `numLateRecordsDropped = 0` và checkpoint thành công.
  - `E13`: Đoạn mã SQL Hopping Window 15m trong Flink UI Operator.

### Bước 8: Kiểm tra tối ưu lưu trữ Lakehouse & DWH Indexing
1. Kiểm tra cấu trúc các bảng trên Lakehouse:
   ```bash
   python3 scripts/inspect_lakehouse.py
   ```
2. Thực hiện Compaction, Z-Order trên Lakehouse:
   ```bash
   python3 scripts/optimize_storage.py
   ```
3. Chạy benchmark đo lường chỉ mục B-Tree trên PostgreSQL DWH:
   ```bash
   python3 scripts/dwh_explain_analyze.py
   ```
- **Kết quả mong đợi**: EXPLAIN ANALYZE giảm từ ~395ms xuống ~2.6ms; Lakehouse gom gọn các file nhỏ.
- **Mã ảnh chụp cần chụp**:
  - `E10`: MinIO Console xem thư mục `ecommerce-lakehouse/gold/fact_user_events/` thấy các file parquet đã gom.
  - `E14`: Terminal sau khi chạy `inspect_lakehouse.py` hiển thị tổng kết các bảng.
  - `E15`: Terminal output của `dwh_explain_analyze.py` thể hiện chuyển từ Seq Scan sang Bitmap Index Scan.

### Bước 9: Đồng bộ siêu dữ liệu & Kiểm định trên DataHub
1. Đồng bộ danh mục 12 datasets và đồ thị phả hệ Lineage:
   ```bash
   make governance-sync
   ```
2. Thực hiện kiểm định các hợp đồng chất lượng dữ liệu:
   ```bash
   make governance-verify
   ```
- **Kết quả mong đợi**: DataHub Web UI (`http://localhost:9002`) hiển thị đủ 12 datasets, phả hệ liên kết từ S3 sang Gold, và các Assertion đạt màu xanh (PASSED).
- **Mã ảnh chụp cần chụp**:
  - `E20`: DataHub UI màn hình Datasets Catalog danh sách 8+ datasets.
  - `E21`: DataHub UI Lineage view của DP1 (Raw -> Bronze).
  - `E22`: DataHub UI Lineage view của DP2 (Bronze -> Silver -> Gold).
  - `E23`: DataHub UI Lineage view của DP3 (Silver -> Features & Labels).
  - `E24`: DataHub UI tab Validations/Assertions hiển thị trạng thái Passing.

### Bước 10: Kiểm tra mô hình Schema & Feast Feature Serving
1. Kiểm tra cấu trúc DWH trên DBeaver/pgAdmin:
   - Kết nối vào PostgreSQL DWH tại `localhost:5432`, database `ecom_dwh`.
2. Kiểm tra phục vụ đặc trưng và đo lường độ trễ:
   ```bash
   python3 scripts/feast_serving_benchmark.py
   ```
- **Kết quả mong đợi**: DBeaver hiển thị đủ 3 schema; độ trễ đọc Redis đạt < 2ms.
- **Mã ảnh chụp cần chụp**:
  - `E25`: DBeaver/pgAdmin sơ đồ ERD hoặc danh sách 3 schemas bronze, silver, gold.
  - `E26`: DBeaver dữ liệu mẫu bảng `gold.dim_product` hiển thị các trường SCD2.
  - `E27`: DBeaver cấu trúc bảng `gold.feat_user_30d` hiển thị đủ `event_timestamp` và `created`.
  - `E28`: DBeaver kết quả truy vấn INNER JOIN `fact_user_events` với `dim_product` và `dim_user`.
  - `E29`: Terminal sau khi chạy materialize gia tăng thể hiện delta nạp mới.
  - `E30`: Terminal sau khi chạy `feast_serving_benchmark.py` hiển thị độ trễ đọc < 2ms.

### Bước 11: Đo lường kích thước Docker Image (`make docker-size`)
Xây dựng và so sánh kích thước 3 biến thể Dockerfile:
```bash
make docker-size
```
- **Kết quả mong đợi**: Bảng so sánh 3 biến thể ghi vào `docs/evidence/docker_sizes.txt`, mức giảm 1.95 GB (-35.6%).
- **Mã ảnh chụp cần chụp**:
  - `E06`: Terminal sau khi chạy `make docker-size` hiển thị bảng so sánh 3 biến thể.

---

## 2. Xử Lý Các Lỗi Thường Gặp (Troubleshooting)

1. **Xung đột cổng kết nối (Port Binding Collision)**:
   - *Hiện tượng*: `bind: address already in use` tại cổng 5432, 6379, 9000 hoặc 8080.
   - *Xử lý*: Kiểm tra dịch vụ đang chiếm cổng bằng `sudo lsof -i :<PORT>` và tạm dừng dịch vụ cục bộ (`sudo systemctl stop postgresql redis`), hoặc chỉnh sửa cổng ngoài trong file `.env`.

2. **Lỗi xác thực MinIO S3 (`AccessDenied` hoặc `Connection Refused`)**:
   - *Hiện tượng*: Spark hoặc PyArrow văng lỗi `AWS Error: Access Denied` khi đọc bucket `ecommerce-raw`.
   - *Xử lý*: Kiểm tra container `ecom_minio` đang ở trạng thái `healthy`; xác nhận `AWS_ACCESS_KEY_ID=minioadmin` và `AWS_SECRET_ACCESS_KEY=minioadmin` trong `.env`.

3. **Thiếu kết nối trong Airflow (`minio_s3_conn` not found)**:
   - *Hiện tượng*: Task DP1 văng lỗi `AirflowNotFoundException: The conn_id minio_s3_conn isn't defined`.
   - *Xử lý*: Chạy lại lệnh `make init-airflow` để nạp tự động các Connection và Variable vào cơ sở dữ liệu của Airflow.

4. **Kafka Broker chưa sẵn sàng khi nộp Flink Job**:
   - *Hiện tượng*: Flink TaskManager báo lỗi `TimeoutException: Failed to update metadata after 60000 ms`.
   - *Xử lý*: Đợi Kafka hoàn tất khởi động (khoảng 15-20 giây sau khi up container) trước khi submit job Flink. Kiểm tra bằng `docker exec ecom_kafka kafka-topics --list --bootstrap-server localhost:9092`.

5. **Tràn bộ nhớ TaskManager hoặc Spark Driver khi chạy dữ liệu lớn**:
   - *Hiện tượng*: Tiến trình Java bị tắt do tín hiệu Out Of Memory (OOMKilled).
   - *Xử lý*: Với máy phát triển cá nhân, chỉ chạy ở chế độ `small` hoặc `medium`. Đảm bảo Docker Desktop / Daemon được cấp tối thiểu 8GB RAM và 4 Cores CPU.
