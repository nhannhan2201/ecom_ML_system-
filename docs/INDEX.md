# HỆ THỐNG MỤC LỤC TÀI LIỆU KỸ THUẬT (DOCS INDEX)

> **Mục tiêu**: Bảng tra cứu tập trung toàn bộ tài liệu kỹ thuật trong thư mục `docs/`, liên kết trực tiếp mục đích nghiệp vụ với các hạng mục đánh giá trong Rubric Mini-coursework DE (EDAI K11 - sheet `edai-1`).

---

## Bảng Tra Cứu Tài Liệu ↔ Hạng Mục Rubric

| File Tài Liệu | Mục Đích Nghiệp Vụ & Kỹ Thuật | Hạng Mục Rubric Chứng Minh | Trạng Thái & Bằng Chứng Đi Kèm |
| :--- | :--- | :--- | :---: |
| [`docs/ARCHITECTURE.md`](ARCHITECTURE.md) | Tổng quan kiến trúc hệ sinh thái dữ liệu đa tầng (Medallion, Lambda Streaming, Serving, Governance). | Tổng quan kiến trúc toàn hệ thống | `DONE` (Sơ đồ ASCII & Mermaid) |
| [`docs/DATA_FLOW.md`](DATA_FLOW.md) | Đặc tả luồng dữ liệu End-to-End từ nguồn REES46 qua Kafka/MinIO/Spark/Flink/Postgres/Feast. | Luồng luân chuyển dữ liệu | `DONE` (Sơ đồ Data Flow chi tiết) |
| [`docs/RUNBOOK.md`](RUNBOOK.md) | Hướng dẫn vận hành, kiểm thử, khởi chạy cụm dịch vụ và xử lý sự cố. | Tính tái lập (Reproducibility) | `DONE` (Kèm target Makefile) |
| [`docs/Data_Generator.md`](Data_Generator.md) | Đặc tả bộ sinh dữ liệu Batch & Stream, tiêm lỗi Skew, High Cardinality, Schema Evolution, Duplicate, Burst, Late Arrival, hướng dẫn 100GB. | **Implement Data Generator** (20.0đ) | `DONE` (Trích xuất từ `docs/evidence/data_profile.md`) |
| [`docs/Spark_Baseline_Report.md`](Spark_Baseline_Report.md) | Báo cáo thực nghiệm Spark Baseline tắt AQE, mô phỏng Task Straggler do Skew và Shuffle Spill do High Cardinality. | **Batch Processing (Baseline)** (2.0đ) | `DONE` (`screenshots/16_spark_baseline_stage14_skew.png`) |
| [`docs/Spark_Optimization_Report.md`](Spark_Optimization_Report.md) | Báo cáo tối ưu Spark Batch: Broadcast Hash Join, AQE Skew Join, HyperLogLog, Delta mergeSchema, deduplication. | **Batch Processing (Optimization)** (14.0đ) | `NEEDS-EVIDENCE` (Chờ ảnh Spark UI `26_...`) |
| [`docs/Flink_Baseline_Report.md`](Flink_Baseline_Report.md) | Báo cáo thực nghiệm Flink Baseline đơn luồng, đo lường nghẽn Burst, Late Arrival Drop 11,996 records, Duplicate. | **Stream Processing (Baseline)** (2.0đ) | `DONE` (`screenshots/10_...`, `screenshots/11_...`) |
| [`docs/Flink_Optimized_Report.md`](Flink_Optimized_Report.md) | Báo cáo tối ưu Flink Streaming: Parallelism 3, Buffer Debloating, FileSystem Checkpoint Exactly-Once, Dedup, Hopping Window 15m. | **Stream Processing (Optimization)** (11.0đ) | `DONE` (`screenshots/12_...`, `13_...`, `14_...`) |
| [`docs/Schema_Design.md`](Schema_Design.md) | Thiết kế Medallion Architecture (Bronze/Silver/Gold), Star Schema DWH, dim_product SCD Type 2, xác nhận `gold.user_labels`. | **Database / DWH Schema** (10.0đ) | `DONE` (`screenshots/17_...`, `18_...`) |
| [`docs/Data_Storage_Optimization.md`](Data_Storage_Optimization.md) | Báo cáo tối ưu Lakehouse (Compaction, Z-Order, Vacuum) & DWH Indexing B-Tree (giảm 99.3% thời gian, 99.9% I/O đĩa). | **Data Storage Optimization** (4.0đ) | `DONE` (`docs/evidence/dwh_explain_analyze.txt`) |
| [`docs/Feature_Store_TTL_Report.md`](Feature_Store_TTL_Report.md) | Báo cáo kiến trúc Feast Feature Store, luận chứng TTL 30 ngày (Batch) vs 2 giờ (Stream), Pusher offline/online, serving SLA 1.58ms. | **Feature Store Architecture** (6.0đ) | `DONE` (`docs/evidence/feast_serving_benchmark.txt`) |
| [`docs/Airflow_Orchestration_Report.md`](Airflow_Orchestration_Report.md) | Báo cáo thiết kế 4 DAGs DP1 -> DP4, cơ chế trigger tuần tự, quản lý Connection/Variable tập trung, append_env bảo mật. | **Airflow Orchestration** (15.0đ) | `DONE` (`screenshots/21_...` đến `24_...`) |
| [`docs/Data_Governance_Report.md`](Data_Governance_Report.md) | Báo cáo quản trị dữ liệu, Data Contracts, Schema Assertions, End-to-End Lineage đồ thị trên DataHub GMS. | **Data Governance** (10.0đ) | `NEEDS-EVIDENCE` (Chờ ảnh DataHub `27_...`, `28_...`) |
| [`docs/DATAHUB.md`](DATAHUB.md) | Hướng dẫn vận hành DataHub GMS, ElasticSearch, Neo4j, REST Emitter CLI. | Phụ trợ Data Governance | `DONE` (Hướng dẫn lệnh sync) |
| [`docs/Docker_Optimization.md`](Docker_Optimization.md) | Báo cáo tối ưu Dockerfile multistage build, Headless JRE, số đo thực nghiệm giảm 1.95 GB (-35.6% dung lượng). | **Engineering Fundamentals** (5.0đ) | `DONE` (`docs/evidence/docker_sizes.txt`) |
| [`docs/EVIDENCE_TODO.md`](EVIDENCE_TODO.md) | Danh sách checklist các ảnh chụp màn hình UI thủ công cần chụp trước buổi bảo vệ đồ án. | Đảm bảo chất lượng bằng chứng | `ACTIVE` (Checklist 4 ảnh UI) |

---

## Thư Mục Dữ Liệu Bằng Chứng (`docs/evidence/`)

Toàn bộ các số liệu định lượng trong các báo cáo trên được đo lường thực tế và xuất ra các tệp chứng minh thô:
- `docs/evidence/docker_sizes.txt`: Đo kích thước Docker Image thực tế giữa Baseline (5.47GB) và Optimized (3.52GB).
- `docs/evidence/data_profile.json` & `docs/evidence/data_profile.md`: Đo lường chi tiết 5 tiêu chí dữ liệu sinh ra (Skew, High Cardinality, Schema Evolution, Duplicate, Volume).
- `docs/evidence/dwh_explain_analyze.txt`: Kế hoạch thực thi `EXPLAIN (ANALYZE, BUFFERS)` của PostgreSQL DWH với 1.4M dòng (Seq Scan 395ms vs Index Scan 2.66ms).
- `docs/evidence/lakehouse_inspection.txt`: Thống kê 6.3M bản ghi Lakehouse và chi tiết các file Parquet đã qua Compaction.
- `docs/evidence/feast_serving_benchmark.txt`: Kết quả đo lường độ trễ Serving Feature thời gian thực từ Redis (Average 1.58ms, Best 0.69ms).
