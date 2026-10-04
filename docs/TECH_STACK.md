# Tech Stack - Công Nghệ & Hạ Tầng

> Bảng thông số chi tiết công nghệ, vai trò, căn cứ lựa chọn và địa chỉ kết nối dịch vụ.

## 1. Bảng Tổng Hợp Công Nghệ

| Tầng kiến trúc | Công cụ | Phiên bản | Vai trò trong dự án | Vì sao chọn (Căn cứ kỹ thuật) | Dùng ở file nào |
|:---|:---|:---|:---|:---|:---|
| **Message Streaming** | Apache Kafka | 7.5.0 (Confluent) | Hàng đợi sự kiện thời gian thực từ Producer | Throughput cao, hỗ trợ phân vùng song song cho streaming job | `docker/docker-compose-kafka.yml`, `src/generator/stream_generator.py` |
| **Stream Processing** | Apache Flink | 1.17.1 (PyFlink) | Xử lý sự kiện theo dòng, tính feature trượt 15m | Hỗ trợ event-time watermark và stateful deduplication mạnh mẽ | `docker/docker-compose-flink.yml`, `src/flink/stream_optimized.py` |
| **Batch Processing** | Apache Spark | 3.5.0 | Chuyển đổi dữ liệu batch lớn từ Bronze sang Gold DWH | Xử lý song song trên bộ nhớ với AQE Skew Join và Broadcast Join | `src/spark/spark_optimized.py`, `src/spark/skew_experiment.py` |
| **Lakehouse Storage** | Delta Lake | 3.0.0 | Định dạng bảng ACID trên tầng MinIO Lakehouse | Hỗ trợ Schema Evolution (`mergeSchema`), Z-Order và Time Travel | `src/spark/spark_optimized.py`, `governance/verify_contracts.py` |
| **Object Storage** | MinIO S3 | RELEASE.2023 | Lưu trữ tập trung Bronze, Silver, Gold Lakehouse | Chuẩn giao thức S3 tương thích hoàn toàn AWS SDK và Hadoop S3A | `docker/docker-compose-minio.yml`, `scripts/inspect_lakehouse.py` |
| **Data Warehouse** | PostgreSQL | 15-alpine | Lưu trữ Star Schema (Kimball DWH) phục vụ BI/Analytics | Hệ quản trị CSDL quan hệ ổn định, hỗ trợ Indexing Composite B-Tree | `docker/docker-compose-postgres.yml`, `scripts/setup_dwh_schemas.py` |
| **Online Feature Store** | Redis | 7-alpine | Lưu trữ đặc trưng trực tuyến phục vụ mô hình inference | Truy xuất key-value trên RAM với độ trễ siêu thấp (< 2ms) | `docker/docker-compose-redis.yml`, `feature_store/features.py` |
| **Feature Store Engine** | Feast | 0.38.0 | Quản lý registry đặc trưng, point-in-time join, materialize | Chuẩn hóa phục vụ tính năng đồng nhất giữa Offline Training và Online Serving | `feature_store/`, `dags/dp4_feast_materialize.py` |
| **Orchestration** | Apache Airflow | 2.7.3 | Điều phối và lập lịch các pipeline DP1, DP2, DP3, DP4 | Hỗ trợ quản lý phụ thuộc đồ thị có hướng (DAG) và retry tự động | `docker/docker-compose-airflow.yml`, `dags/` |
| **Data Governance** | DataHub | 0.13.0 | Quản lý catalog tập trung, phả hệ (lineage) và hợp đồng dữ liệu | Tích hợp giao diện quản trị hiện đại, hỗ trợ Metadata Change Proposals | `docker/docker-compose-datahub.yml`, `governance/` |
| **Containerization** | Docker & Compose | Compose v2 | Đóng gói và cô lập toàn bộ môi trường thực thi | Tái hiện hệ thống nhất quán 100% giữa máy cá nhân và máy chấm bài | `docker/` |

---

## 2. Bảng Cổng & Địa Chỉ Dịch Vụ

| Dịch vụ | Giao diện | Địa chỉ URL trên Host | Thông tin xác thực mặc định |
|:---|:---|:---|:---|
| **Apache Airflow** | Web UI | `http://localhost:8080` | `airflow` / `airflow` |
| **Apache Flink** | Web Dashboard | `http://localhost:8081` | Không yêu cầu xác thực |
| **Apache Spark** | Web UI | `http://localhost:4040` | Mở khi SparkSession đang hoạt động |
| **MinIO Lakehouse** | Console UI | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| **MinIO S3 API** | S3 Endpoint | `http://localhost:9000` | S3 Key / Secret: `minioadmin` |
| **DataHub** | Web Frontend | `http://localhost:9002` | `datahub` / `datahub` |
| **DataHub GMS** | REST API | `http://localhost:8089` | REST endpoint nội bộ |
| **PostgreSQL DWH** | JDBC / SQL | `localhost:5432` (db: `ecom_dwh`) | `postgres` / `postgres` |
| **Redis** | Key-Value Store | `localhost:6379` | Không yêu cầu mật khẩu dev |
| **Apache Kafka** | External Broker | `localhost:9092` | PLAINTEXT |

---

## 3. Lựa Chọn Thay Thế Đã Cân Nhắc

1. **Spark Master (`local[*]` thay vì Standalone Cluster):** Phù hợp với tài nguyên máy cá nhân và rubric mini-coursework mà vẫn bộc lộ đầy đủ bản chất của Spark UI (Shuffle Spill, AQE, Skew Join).
2. **Feast Offline Store (`FileSource` Parquet thay vì BigQuery/Snowflake):** Lưu trực tiếp trên MinIO S3 Lakehouse giúp hệ thống tự chủ 100% offline, không phát sinh chi phí cloud.
3. **MinIO thay vì AWS S3 thật:** Giữ nguyên giao thức AWS SDK chuẩn mực mà không phụ thuộc vào kết nối mạng bên ngoài hay tài khoản đám mây.
4. **PostgreSQL DWH thay vì ClickHouse/Redshift:** Đáp ứng trọn vẹn yêu cầu Star Schema Kimball và benchmark B-Tree Indexing theo rubric với mức tiêu hao tài nguyên thấp.
