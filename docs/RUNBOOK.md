# CẨM NANG VẬN HÀNH TOÀN DIỆN (SYSTEM RUNBOOK)
**Dự án:** E-Commerce Real-Time Purchase Propensity Prediction System (`ecom_ML_system`)  
**Khóa học:** EDAI K11 - Data Engineering (Mini-coursework)

---

## 1. MỤC TIÊU VẬN HÀNH
Tài liệu này cung cấp các lệnh chính xác để vận hành hệ thống trong 2 kịch bản:
1. **LOCAL DEVELOPMENT:** Phát triển và kiểm thử trên máy cá nhân (`small` 1M rows hoặc `medium` 5M rows).
2. **CLOUD 100GB BENCHMARK:** Mở rộng benchmark quy mô lớn (>=100GB) trên máy chủ đám mây đủ dung lượng ổ đĩa.

---

## 2. KỊCH BẢN 1: LOCAL DEVELOPMENT (MÁY CÁ NHÂN)

### Bước 1: Khởi động toàn bộ hạ tầng Docker
```bash
# Khởi động các cụm dịch vụ theo thứ tự phụ thuộc
docker compose -f docker/docker-compose-kafka.yml up -d
docker compose -f docker/docker-compose-minio.yml up -d
docker compose -f docker/docker-compose-postgres.yml up -d
docker compose -f docker/docker-compose-redis.yml up -d
docker compose -f docker/docker-compose-flink.yml up -d
docker compose -f docker/docker-compose-airflow.yml up -d
docker compose -f docker/docker-compose-datahub.yml up -d

# Kiểm tra trạng thái toàn bộ containers
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

### Bước 2: Sinh dữ liệu mẻ (Batch Generator - Chế độ Small)
Mặc định sinh **1,000,000 dòng** (~131 MB) chia đều cho 2 giai đoạn Schema Evolution và tiêm 2% duplicate:
```bash
# Kích hoạt môi trường Python (ví dụ: conda activate learn_database)
python src/generator/batch_generator.py --mode small
```
*Kết quả:* Tạo ra 2 file trên MinIO:
- `s3://ecommerce-raw/batch/raw_events_old.csv` (01/10 -> 15/10: 9 cột nguyên bản)
- `s3://ecommerce-raw/batch/raw_events_new.csv` (16/10 -> 25/10: 10 cột có `discount_percent`)

*(Tùy chọn) Chế độ Integration Test (Medium 5M dòng):*
```bash
python src/generator/batch_generator.py --mode medium
```

### Bước 3: Phát sinh luồng Streaming vào Kafka
```bash
# Phát sinh dữ liệu thời gian thực (tốc độ 100 msg/s, 5% late arrivals, 1.5% duplicates, burst traffic)
python src/generator/stream_generator.py --rate 100 --burst-multiplier 10
```

### Bước 4: Khởi chạy Flink Streaming Job
```bash
# Submit Flink Job lên Flink JobManager (sử dụng container hoặc script submit)
bash scripts/submit_flink_job.sh
```
*Kiểm tra:* Mở Flink Dashboard tại `http://localhost:8081` để theo dõi Job Graph, Watermark và Checkpoints.

### Bước 5: Chạy các Pipelines Điều Phối Airflow
```bash
# 1. Kích hoạt DP1: Raw CSV -> Bronze Lakehouse
docker exec airflow_scheduler airflow dags trigger dp1_raw_to_bronze

# 2. Sau khi DP1 hoàn thành, DP2 tự động được trigger (hoặc kích hoạt thủ công):
docker exec airflow_scheduler airflow dags trigger dp2_bronze_to_silver_and_gold

# 3. Sau khi DP2 hoàn thành, DP3 tự động tính toán 30-day offline features:
docker exec airflow_scheduler airflow dags trigger dp3_compute_offline_features

# 4. Kích hoạt DP4: Feast Materialization nạp đặc trưng sang Redis:
docker exec airflow_scheduler airflow dags trigger dp4_feast_materialize
```

### Bước 6: Kiểm tra Feature Serving trên Redis
```bash
cd feature_store
python test_serving.py
cd ..
```
*Kết quả:* Vector đặc trưng được truy xuất từ Redis Online Store trong thời gian `< 5ms`.

### Bước 7: Đồng bộ hóa & Kiểm định trên DataHub
```bash
# 1. Đồng bộ toàn bộ Danh mục 12 datasets và sơ đồ phả hệ (Lineage)
# Trên máy Host:
python governance/sync_catalog.py --gms-url http://localhost:8089

# 2. Kiểm định hợp đồng chất lượng dữ liệu thực tế (Sample-based 50k rows trên Delta snapshot)
python governance/verify_contracts.py --gms-url http://localhost:8089
```
*Kiểm tra:* Mở Web UI `http://localhost:9002` (user: `datahub`, pass: `datahub`) kiểm tra Lineage và kết quả Assertions.

---

## 3. KỊCH BẢN 2: CLOUD 100GB BENCHMARK (MÁY CHỦ ĐÁM MÂY)

> [!IMPORTANT]
> Chế độ Benchmark >= 100GB yêu cầu máy chủ có ổ đĩa trống tối thiểu **150 GB** (cho MinIO data volume và Delta tables). Không chạy chế độ này trên máy tính cá nhân nếu không đủ dung lượng.

### Cấu hình môi trường đám mây
Đặt các biến môi trường tới các dịch vụ hạ tầng đám mây (hoặc MinIO trên máy chủ):
```bash
export MINIO_ENDPOINT="http://<cloud-minio-host>:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export KAFKA_BOOTSTRAP_SERVERS="<cloud-kafka-host>:9092"
# DataHub GMS endpoint (Host: http://<host>:8089; Docker nội bộ: http://datahub-gms:8080)
export DATAHUB_GMS_URL="http://<cloud-datahub-host>:8089"
```

### Lệnh thực thi Benchmark >= 100GB
Sử dụng cờ `--mode full` và chỉ định dung lượng đích `--target-size-gb 100`:
```bash
python src/generator/batch_generator.py \
    --mode full \
    --target-size-gb 100
```

### Cơ chế hoạt động của Benchmark Mode:
1. **Zero-OOM Chunked Processing:** Đọc và xử lý theo từng khối `chunk_size = 250,000` dòng, không bao giờ nạp toàn bộ 100GB vào RAM.
2. **Deterministic Time-Shifted Replay:** Tận dụng 5.3GB dữ liệu gốc từ REES46, mở rộng workload qua các replica thời gian (+30 ngày, +60 ngày...) bảo toàn phân phối skew, category, brand và tỷ lệ duplicate tự nhiên.
3. **Chunked Part Files:** Dữ liệu được ghi thành các part files ~500MB trên MinIO:
   - `s3://ecommerce-raw/batch/raw_events_old_part-00000.csv`, `raw_events_old_part-00001.csv` ... (9 cột)
   - `s3://ecommerce-raw/batch/raw_events_new_part-00000.csv`, `raw_events_new_part-00001.csv` ... (10 cột)
4. **Authoritative Stopping Condition:** Tự động dừng chính xác khi tổng số bytes ghi đạt mốc mục tiêu `100 * 1024 * 1024 * 1024` bytes.

---

## 4. BẢNG TRA CỨU WEB DASHBOARDS & THÔNG TIN ĐĂNG NHẬP

| Dịch vụ | Địa chỉ Web UI | Thông tin đăng nhập | Mục đích kiểm tra |
|:---|:---|:---|:---|
| **MinIO Console** | `http://localhost:9001` | `minioadmin` / `minioadmin` | Kiểm tra buckets `ecommerce-raw` và `ecommerce-lakehouse` |
| **Apache Airflow** | `http://localhost:8080` | `admin` / `admin` | Kiểm tra trạng thái thực thi DAGs và Task logs |
| **Apache Flink** | `http://localhost:8081` | Không yêu cầu | Theo dõi Streaming Job, Checkpoints, Backpressure |
| **DataHub Frontend** | `http://localhost:9002` | `datahub` / `datahub` | Xem Catalog, Lineage đồ thị, Data Contracts |
| **PostgreSQL DWH** | `localhost:5432` | `postgres` / `postgres` (db: `ecom_dwh`) | Kiểm tra bảng sự kiện và khóa ngoại Star Schema |
| **Redis** | `localhost:6379` | Không mật khẩu | Kiểm tra các Online Feature Keys |
| **Spark Master / UI** | `http://localhost:4040` (khi chạy) | Không yêu cầu | Xem Stage Execution, Shuffle Read/Write, Disk Spill |
