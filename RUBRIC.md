# BẢNG THEO DÕI RUBRIC ĐỒ ÁN EDAI K11 - DATA ENGINEERING

**Tài liệu gốc:** `rubic/EDAI K11 - DE.xlsx` → Sheet: `edai-1 (50%)` (Thang điểm: 100 điểm)  
**Quy tắc:** Tuyệt đối không bịa số liệu. Trạng thái phản ánh chính xác kết quả đo lường và kiểm thử thực tế.

---

## 1. BẢNG TIẾN ĐỘ THEO DÕI RUBRIC (CORE DE: 80 ĐIỂM + NOVEL IDEAS: 20 ĐIỂM)

| # | Hạng mục Rubric | Điểm | Trạng thái | File bằng chứng hoặc code | Việc còn thiếu / Ghi chú |
| :-: | :--- | :-: | :---: | :--- | :--- |
| **1** | **Engineering Fundamentals: Docker Compose** | 2.0 | **DONE** | `docker/docker-compose-*.yml` (7 stack: kafka, minio, postgres, redis, flink, airflow, datahub) | Không. Đã xác nhận `docker compose config` hợp lệ cho cả 7 stack. |
| **2** | **Engineering Fundamentals: Tối ưu Dockerfile** | 3.0 | **DONE** | `docker/Dockerfile.airflow`, `docs/Docker_Optimization.md`, `docs/evidence/docker_sizes.txt` | Không. Đã đo thật: Baseline 5.47 GB vs Multistage 3.52 GB (-1.95 GB / -35.6%). |
| **3** | **Data Generator: Simulate Skew** | 2.0 | **DONE** | `src/generator/batch_generator.py`, `docs/evidence/data_profile.md` | Không. Kết hợp skew tự nhiên REES46 và chế độ opt-in key skew (`--skewed`). |
| **4** | **Data Generator: Simulate High Cardinality** | 2.0 | **DONE** | `src/generator/batch_generator.py`, `docs/evidence/data_profile.md` | Không. Đã đo thật: 163k+ user_id, 63k+ product_id, cardinality ratio 0.163. |
| **5** | **Data Generator: Simulate Schema Evolution** | 2.0 | **DONE** | `src/generator/batch_generator.py`, `tests/test_generator.py` | Không. Part 1 (9 cột), Part 2 (10 cột với discount_percent). Đã có unit test tự động. |
| **6** | **Data Generator: Simulate Duplicate Rate** | 2.0 | **DONE** | `src/generator/batch_generator.py`, `tests/test_generator.py` | Không. Tiêm ~2% duplicate tách biệt độc lập với replay. Đã có unit test tự động. |
| **7** | **Data Generator: Upload to MinIO** | 2.0 | **DONE** | `src/generator/batch_generator.py`, `data/generation_manifest.json` | Không. Upload trực tiếp dạng chunked streaming lên `s3://ecommerce-raw/batch/`. |
| **8** | **Data Generator: Quy mô >= 100GB** | - | **PARTIAL** | `src/generator/batch_generator.py` (`--mode full --dry-run`) | Cần chạy thực tế trên máy ảo/cloud đủ đĩa (>130GB trống). Đã có `--dry-run` kiểm tra an toàn đĩa. |
| **9** | **Streaming Generator: Late Arrival** | 2.0 | **DONE** | `src/generator/stream_generator.py`, `tests/test_generator.py` | Không. Cơ chế `late_buffer` dựa theo event-time (trễ 5-10 phút). Đã có unit test tự động. |
| **10** | **Streaming Generator: Streaming Duplicate** | 2.0 | **DONE** | `src/generator/stream_generator.py`, `config/generator_config.yaml` | Không. Tiêm 1.5% duplicate giữ nguyên partition ordering với `key=user_id`. |
| **11** | **Spark: Baseline (chưa tối ưu)** | 2.0 | **NEEDS-EVIDENCE** | `src/spark/spark_baseline.py`, `docs/Spark_Baseline_Report.md` | Cần chụp ảnh Spark UI Baseline (Stage/Task skew & spill) theo `docs/EVIDENCE_TODO.md`. |
| **12** | **Spark: Xử lý Data Skew** | 3.0 | **NEEDS-EVIDENCE** | `src/spark/spark_optimized.py`, `docs/Spark_Optimization_Report.md` | Cần chụp ảnh Spark UI so sánh Salting + Broadcast Join + AQE Skew Join. |
| **13** | **Spark: Xử lý Schema Evolution** | 3.0 | **DONE** | `src/spark/spark_optimized.py` (`mergeSchema=true`), `docs/Data_Storage_Optimization.md` | Không. Delta mergeSchema tự động hợp nhất 11 cột Bronze từ 2 định dạng CSV. |
| **14** | **Spark: Xử lý Offline Problem (Deduplication)** | 3.0 | **DONE** | `src/spark/spark_optimized.py`, `governance/verify_contracts.py` | Không. Khử trùng lặp trên bộ khóa `(user_id, event_time, product_id, event_type)` sạch 100%. |
| **15** | **Spark: Tích hợp vào Data Pipelines** | 2.0 | **DONE** | `dags/dp1_raw_to_bronze.py`, `dags/dp2_bronze_to_silver_and_gold.py`, `dags/dp3_compute_offline_features.py` | Không. Điều phối tự động qua Airflow BashOperator với credential bảo mật truyền qua env. |
| **16** | **Flink: Baseline (chưa tối ưu)** | 2.0 | **NEEDS-EVIDENCE** | `src/flink/stream_baseline.py`, `docs/Flink_Baseline_Report.md` | Cần chụp ảnh Flink Web Dashboard thể hiện Backpressure khi gặp Burst Traffic. |
| **17** | **Flink: Xử lý Late Arrival** | 3.0 | **NEEDS-EVIDENCE** | `src/flink/stream_optimized.py`, `docs/Flink_Optimized_Report.md` | Cần chụp ảnh Flink Dashboard thể hiện 0 dropped late records với Watermark 15 phút. |
| **18** | **Flink: Xử lý Streaming Duplicate** | 3.0 | **DONE** | `src/flink/stream_optimized.py` | Không. Khử trùng lặp trạng thái qua Window Row_number() Top-1 Dedup View. |
| **19** | **Flink: Xử lý Windowing** | 2.0 | **DONE** | `src/flink/stream_optimized.py` | Không. Hopping Window `HOP(row_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE)` sinh 4 features. |
| **20** | **Lưu trữ: Lakehouse Optimization** | 3.0 | **DONE** | `scripts/optimize_storage.py`, `docs/evidence/lakehouse_inspection.txt` | Không. Đã đo thật: Compaction gom file nhỏ thành 3 file ~42MB, Z-Order theo user_id, Vacuum. |
| **21** | **Lưu trữ: Datawarehouse Indexing** | 3.0 | **DONE** | `scripts/dwh_explain_analyze.py`, `docs/evidence/dwh_explain_analyze.txt` | Không. Đã đo thật EXPLAIN ANALYZE: Seq Scan 395.70ms -> B-Tree Index 2.66ms (nhanh hơn 148.8x). |
| **22** | **Airflow: Pipeline DP1 (Ingest & Validate)** | - | **NEEDS-EVIDENCE** | `dags/dp1_raw_to_bronze.py`, `tests/test_dags_import.py` | Cần chụp ảnh Airflow Grid/Graph View khi DP1 Ingest >> Validate >> Trigger thành công. |
| **23** | **Airflow: Pipeline DP2 (Bronze -> Silver & Gold)** | - | **NEEDS-EVIDENCE** | `dags/dp2_bronze_to_silver_and_gold.py`, `tests/test_dags_import.py` | Cần chụp ảnh Airflow UI khi DP2 hoàn tất khử trùng lặp và xây dựng Star Schema. |
| **24** | **Airflow: Pipeline DP3 (Features & Labels)** | - | **NEEDS-EVIDENCE** | `dags/dp3_compute_offline_features.py`, `tests/test_dags_import.py` | Cần chụp ảnh Airflow UI khi DP3 tính toán feat_user_30d và user_labels chuẩn Feast. |
| **25** | **Airflow: Pipeline DP4 (Feast Materialize)** | - | **DONE** | `dags/dp4_feast_materialize.py`, `docs/evidence/feast_serving_benchmark.txt` | Không. Pipeline mở rộng đồng bộ Redis Online Store; đo độ trễ phục vụ thật: 1.58ms. |
| **26** | **Quản trị DataHub: DP1 Lineage & Assertions** | - | **NEEDS-EVIDENCE** | `governance/sync_catalog.py`, `governance/verify_contracts.py` | Cần chụp ảnh DataHub UI Lineage (Raw -> Bronze) và thẻ Assertions màu XANH (PASSED). |
| **27** | **Quản trị DataHub: DP2 Lineage & Assertions** | - | **NEEDS-EVIDENCE** | `governance/catalog.py`, `governance/verify_contracts.py` | Cần chụp ảnh DataHub UI Lineage (Bronze -> Silver & Gold) và hợp đồng kiểm định SCD2. |
| **28** | **Quản trị DataHub: DP3 Lineage & Assertions** | - | **NEEDS-EVIDENCE** | `governance/catalog.py`, `governance/verify_contracts.py` | Cần chụp ảnh DataHub UI Lineage (Silver -> Features) và kiểm định nhãn nhị phân [0, 1]. |
| **29** | **Thiết kế Schema: Mô hình hóa toàn bộ Zones** | - | **DONE** | `docs/Schema_Design.md`, `docs/DATA_FLOW.md` | Không. Đầy đủ sơ đồ ERD, Data Flow, từ điển dữ liệu cho 8 bảng qua 4 tầng. |
| **30** | **Thiết kế Schema: SCD Type 2 cho Dimension** | - | **DONE** | `docs/Schema_Design.md`, `src/spark/spark_optimized.py` | Không. `dim_product` chuẩn SCD2 (`valid_from_ts`, `valid_to_ts`, `is_current`, `product_sk`). |
| **31** | **Thiết kế Schema: Bảng Feature (`feat_`)** | - | **DONE** | `feature_store/features.py`, `docs/Feature_Store_TTL_Report.md` | Không. `feat_user_30d` và `feat_user_stream` có đầy đủ `event_timestamp` và `created`. |
| **32** | **Thiết kế Schema: Mối quan hệ Dim & Fact** | - | **DONE** | `docs/Schema_Design.md`, `scripts/setup_dwh_schemas.py` | Không. Mô hình Kimball Star Schema: `fact_user_events` liên kết FK surrogate key `product_sk`. |
| **33** | **Thiết kế Schema: Quy ước đặt tên (Naming)** | - | **DONE** | Toàn bộ repo (`raw_`, `stg_`, `dim_`, `fact_`, `feat_`) | Không. Nhất quán 100% giữa code Spark, DWH Postgres, Delta Lakehouse và DataHub Catalog. |
| **34** | **Kiểm thử tự động & Coverage** | - | **DONE** | `tests/`, `pyproject.toml`, `Makefile` | Đã thiết lập 22 unit tests tự động; độ phủ kiểm thử đo lường thực tế đạt **15%** (1405 stmts). |
| **35** | **Novel Idea 1: Debezium CDC Pipeline** | 10.0 | **OUT-OF-SCOPE** | - | Dành cho đồ án tốt nghiệp cuối khóa (Final Coursework). Tạm tính: 0/10. |
| **36** | **Novel Idea 2: Trino Distributed Query** | 10.0 | **OUT-OF-SCOPE** | - | Dành cho đồ án tốt nghiệp cuối khóa (Final Coursework). Tạm tính: 0/10. |

---

## 2. TỔNG KẾT ĐIỂM SỐ THEO RUBRIC

* **Tổng điểm Core Data Engineering (EDAI K11 Mini-Coursework):** **70 - 75 / 80 điểm** (đã hoàn thiện 100% mã nguồn, đo lường số liệu thật; các hạng mục `NEEDS-EVIDENCE` chỉ cần chụp màn hình UI theo hướng dẫn tại `docs/EVIDENCE_TODO.md` để hoàn tất 80/80 điểm tối đa).
* **Novel Ideas:** **0 / 20 điểm** (trạng thái `OUT-OF-SCOPE`, tuân thủ chỉ đạo không mở rộng sang đồ án cuối khóa).
* **Quy mô 100GB Benchmark:** Đã hoàn thành kiến trúc Zero-OOM Streaming Replay và dry-run, được đánh giá `PARTIAL` do giới hạn phần cứng máy tính cá nhân.

---

## 3. SẮP TỚI: KẾ HOẠCH ĐỒ ÁN CUỐI KHÓA (FINAL COURSEWORK)

Các khối kiến trúc nâng cao phục vụ giai đoạn tốt nghiệp cuối khóa bao gồm:
1. **Web API Service:** Triển khai FastAPI microservice tại `src/api/` phục vụ dự đoán propensity thời gian thực, tích hợp Feast Online Store Redis.
2. **Kubernetes & Cloud Orchestration:** Đóng gói Helm Charts triển khai hệ thống trên cụm k8s (EKS/GKE), tích hợp autoscaling HPA.
3. **CI/CD Automation:** Xây dựng Jenkins / GitHub Actions pipeline tự động chạy lint (ruff), unit tests (pytest), build Docker multistage và kiểm tra hợp đồng DataHub.
4. **Machine Learning Lifecycle (Kubeflow / MLflow):** Tự động hóa huấn luyện mô hình dự đoán xu hướng mua hàng (Purchase Propensity Classifier) với Model Registry và Artifact Tracking.
5. **Full Observability & Monitoring:** Tích hợp Prometheus, Grafana, OpenTelemetry theo dõi thông lượng Kafka, trễ Flink, tài nguyên Spark và độ trễ Feast Serving SLA (<5ms).
