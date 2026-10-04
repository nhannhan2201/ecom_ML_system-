# BẢNG THEO DÕI RUBRIC ĐỒ ÁN EDAI K11 - DATA ENGINEERING

Tài liệu gốc: `rubic/EDAI K11 - DE.xlsx` -> Sheet: `edai-1 (50%)` (Thang điểm: 100 điểm).  
Quy tắc: Không bịa số liệu. Mọi điểm số phản ánh đúng trạng thái mã nguồn và file evidence.

---

## 1. BẢNG CHI TIẾT THEO DÒNG RUBRIC GỐC

| STT | Phân mục | Hạng mục Rubric | Điểm | Trạng thái | File bằng chứng / Mã nguồn | Ghi chú |
| :-: | :--- | :--- | :-: | :---: | :--- | :--- |
| 1 | Engineering Fundamentals | Có sử dụng Docker & Docker Compose | 2.0 | **DONE** | `docker/docker-compose-*.yml` | 7 file compose cho kafka, minio, postgres, redis, flink, airflow, datahub |
| 2 | Engineering Fundamentals | Optimize Dockerfile (ví dụ multistage build) | 3.0 | **DONE** | `docker/Dockerfile.airflow` | Đã đo thực tế: Baseline 5.47 GB vs Multistage 3.52 GB (-1.95 GB) trong `docs/evidence/docker_sizes.txt` |
| 3 | Implement Data Generator | Simulate skew | 2.0 | **DONE** | `src/generator/batch_generator.py` | Đã thêm user_id offset và price jitter, tỉ lệ skew top-k theo manifest trong `docs/evidence/data_profile.md` |
| 4 | Implement Data Generator | Simulate schema evolution | 2.0 | **DONE** | `src/generator/batch_generator.py` | Part 1 (9 cột), Part 2 (10 cột có discount_percent), kiểm thử tự động tại `tests/test_generator.py` |
| 5 | Implement Data Generator | Simulate another offline data problem (Ví dụ: 2% duplicate rate) | 2.0 | **DONE** | `src/generator/batch_generator.py` | Tiêm 2% duplicate độc lập với replay, kiểm thử tự động tại `tests/test_generator.py` |
| 6 | Implement Data Generator | Store data after generating so that we can ingest to Bronze zone later | 2.0 | **DONE** | `src/generator/batch_generator.py` | Upload chunked streaming lên `s3://ecommerce-raw/batch/` qua MinIO S3 API |
| 7 | Implement Data Generator | Simulate late arrivals | 2.0 | **DONE** | `src/generator/stream_generator.py` | Lớp LateEventBuffer độc lập, độ trễ 5-10 phút, báo cáo tại `docs/evidence/stream_profile.md` |
| 8 | Implement Data Generator | Simulate another streaming data problem (Ví dụ: 1.5% duplicate rate) | 2.0 | **DONE** | `src/generator/stream_generator.py` | Tiêm 1.5% duplicate streaming, ghi thống kê vào `data/stream_manifest.json` |
| 9 | Processing Jobs | Spark Baseline (without optimization) | 2.0 | **NEEDS-EVIDENCE** | `src/spark/spark_baseline.py` | Cần chụp ảnh Spark UI Baseline (Stage/Task skew & spill) theo mã E07 |
| 10 | Processing Jobs | Spark Handle skew with explanation | 3.0 | **NEEDS-EVIDENCE** | `src/spark/skew_experiment.py` | Đã đo 3 biến thể trong `docs/evidence/spark_skew_experiment.md`, cần chụp ảnh E08-E10 |
| 11 | Processing Jobs | Spark Handle schema evolution with explanation | 3.0 | **DONE** | `src/spark/spark_optimized.py` | Delta mergeSchema tự động hợp nhất 11 cột Bronze từ 2 định dạng CSV |
| 12 | Processing Jobs | Spark Handle other offline data problem with explanation | 3.0 | **DONE** | `src/spark/spark_optimized.py` | Khử trùng lặp trên bộ khóa logic (user_id, event_time, product_id, event_type) |
| 13 | Processing Jobs | Spark job được tích hợp vào các data pipeline | 2.0 | **DONE** | `dags/dp1_raw_to_bronze.py` | Điều phối qua Airflow BashOperator, credential qua env, kết nối minio_s3_conn |
| 14 | Processing Jobs | Flink Baseline (without optimization) | 2.0 | **NEEDS-EVIDENCE** | `src/flink/stream_baseline.py` | Cần chụp ảnh Flink Web Dashboard thể hiện Backpressure khi burst traffic (E11) |
| 15 | Processing Jobs | Flink Handle late arrival with explanation | 3.0 | **NEEDS-EVIDENCE** | `src/flink/stream_optimized.py` | Cần chụp ảnh Flink UI 0 dropped late records với Watermark 15 phút (E12) |
| 16 | Processing Jobs | Flink Handle other streaming problem with explanation | 3.0 | **DONE** | `src/flink/stream_optimized.py` | Khử trùng lặp qua Window Row_number() Top-1 Dedup View |
| 17 | Processing Jobs | Flink Window processing | 2.0 | **DONE** | `src/flink/stream_optimized.py` | Hopping Window HOP(row_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE) |
| 18 | Data Storage | Lakehouse (compaction, z-order, partitioning) | 3.0 | **DONE** | `scripts/optimize_storage.py` | Đã đo thực tế: Compaction gom file nhỏ, Z-Order theo user_id, Vacuum an toàn 168h |
| 19 | Data Storage | Datawarehouse (indexing) | 3.0 | **DONE** | `scripts/dwh_explain_analyze.py` | Đã đo thực tế EXPLAIN ANALYZE: Seq Scan 395.70ms -> B-Tree Index 2.66ms (nhanh hơn 148.8x) |
| 20 | Data Pipeline Orchestration | DP1 Ingest stage | 2.0 | **NEEDS-EVIDENCE** | `dags/dp1_raw_to_bronze.py` | Cần chụp ảnh Airflow Grid/Graph View DP1 thành công (E14) |
| 21 | Data Pipeline Orchestration | DP1 Validate stage | 2.0 | **NEEDS-EVIDENCE** | `governance/verify_contracts.py` | Cần chụp ảnh Airflow task validate_bronze_contracts thành công (E15) |
| 22 | Data Pipeline Orchestration | DP2 Ingest stage | 2.0 | **NEEDS-EVIDENCE** | `dags/dp2_bronze_to_silver_and_gold.py` | Cần chụp ảnh Airflow Grid/Graph View DP2 thành công (E16) |
| 23 | Data Pipeline Orchestration | DP2 Validate stage | 2.0 | **NEEDS-EVIDENCE** | `governance/verify_contracts.py` | Cần chụp ảnh Airflow task validate_silver_and_gold thành công (E17) |
| 24 | Data Pipeline Orchestration | DP3 Ingest stage | 2.0 | **NEEDS-EVIDENCE** | `dags/dp3_compute_offline_features.py` | Cần chụp ảnh Airflow Grid/Graph View DP3 thành công (E18) |
| 25 | Data Pipeline Orchestration | DP3 Validate stage | 2.0 | **NEEDS-EVIDENCE** | `governance/verify_contracts.py` | Cần chụp ảnh Airflow task validate_features_and_labels thành công (E19) |
| 26 | Data Governance | DP1 Lineage between pipeline and tables | 2.0 | **NEEDS-EVIDENCE** | `governance/sync_catalog.py` | Cần chụp ảnh DataHub UI Lineage DP1 (E26) |
| 27 | Data Governance | DP1 Data validation | 2.0 | **NEEDS-EVIDENCE** | `governance/verify_contracts.py` | Cần chụp ảnh DataHub UI Assertion Bronze PASSED (E29) |
| 28 | Data Governance | DP2 Lineage between pipeline and tables | 2.0 | **NEEDS-EVIDENCE** | `governance/sync_catalog.py` | Cần chụp ảnh DataHub UI Lineage DP2 (E27) |
| 29 | Data Governance | DP2 Data validation | 2.0 | **NEEDS-EVIDENCE** | `governance/verify_contracts.py` | Cần chụp ảnh DataHub UI Assertion Silver/Gold PASSED (E30) |
| 30 | Data Governance | DP3 Lineage between pipeline and tables | 2.0 | **NEEDS-EVIDENCE** | `governance/sync_catalog.py` | Cần chụp ảnh DataHub UI Lineage DP3 (E28) |
| 31 | Data Governance | DP3 Data validation | 2.0 | **NEEDS-EVIDENCE** | `governance/verify_contracts.py` | Cần chụp ảnh DataHub UI Assertion Features/Labels PASSED (E30) |
| 32 | Documentation | Visualize tables on all zones | 2.0 | **NEEDS-EVIDENCE** | `docs/Schema_Design.md` | Cần chụp ảnh sơ đồ bảng 4 zones (E21) |
| 33 | Documentation | Dim table with SCD 2 (valid_from_ts, valid_to_ts, is_current) | 2.0 | **NEEDS-EVIDENCE** | `src/spark/spark_optimized.py` | Cần chụp ảnh bảng dim_product SCD2 có đầy đủ trường thời gian (E23) |
| 34 | Documentation | Feature tables (feat_ tables) with event_timestamp and created | 2.0 | **NEEDS-EVIDENCE** | `feature_store/features.py` | Cần chụp ảnh bảng feat_user_30d có event_timestamp và created (E24) |
| 35 | Documentation | Relationship between dim & fact tables | 2.0 | **NEEDS-EVIDENCE** | `docs/Schema_Design.md` | Cần chụp ảnh ERD quan hệ Star Schema Dim & Fact từ DWH/DBeaver (E22) |
| 36 | Documentation | Naming convention (raw_, stg_, dim_, fact_, feat_) | 2.0 | **DONE** | Toàn bộ repo | Nhất quán 100% trên Delta Lakehouse, PostgreSQL DWH, Feast và DataHub |
| 37 | Novel ideas | Idea 1 | 10.0 | **TODO (chưa thực hiện, quyết định của sinh viên)** | - | Nghiên cứu mở rộng công cụ hoặc kỹ thuật ngoài chương trình EDAI (ví dụ CDC hoặc Trino) |
| 38 | Novel ideas | Idea 2 | 10.0 | **TODO (chưa thực hiện, quyết định của sinh viên)** | - | Nghiên cứu mở rộng công cụ hoặc kỹ thuật ngoài chương trình EDAI |

---

## 2. TỔNG KẾT ĐIỂM THEO TRẠNG THÁI

- **DONE (Đã hoàn thành mã nguồn & có kết quả đo):** 38.0 / 100 điểm
- **NEEDS-EVIDENCE (Mã nguồn đã chạy, chờ chụp ảnh màn hình theo `docs/EVIDENCE_TODO.md`):** 42.0 / 100 điểm
- **PARTIAL:** 0.0 / 100 điểm
- **TODO (Chưa thực hiện, quyết định của sinh viên cho Novel Ideas):** 20.0 / 100 điểm

Tổng điểm Core Data Engineering (36 mục): **80.0 / 80.0 điểm** (100% mã nguồn và logic pipeline đã hoàn thiện, đạt điểm tối đa khi bổ sung ảnh chụp theo `docs/EVIDENCE_TODO.md`).  
Tổng điểm tối đa toàn bộ rubric: **100.0 / 100.0 điểm**.
