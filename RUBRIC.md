# Theo dõi yêu cầu rubric

Nguồn: rubic/EDAI K11 - DE.xlsx, sheet edai-1 (50%); tương ứng rubic/EDAI K11 - MLE.xlsx, sheet de. Bỏ qua Novel Ideas. Các phần final/MLE sẽ xét sau luồng dữ liệu nền tảng.

Có code không có nghĩa đã implement đúng hoặc đã chạy. Không dùng kết quả cũ để quy điểm. Mỗi milestone sẽ cập nhật phần đã kiểm chứng, lệnh và kết quả thật. Offline generator yêu cầu tối thiểu 100GB; chưa có bằng chứng chạy lại ở scale đó.

Baseline model đã chốt dùng bốn feature 15 phút; rubric không bắt buộc cửa sổ 30 ngày. Điều này không bỏ các tiêu chí feature store: offline feature history, incremental materialize offline→online, stream push vào offline và online, temporal columns, training/label join vẫn phải thực hiện và kiểm chứng. Xem [WorkFlow](docs/WorkFlow.md); thiết kế này chưa được code/runtime xác nhận.

| Dòng workbook | Nhóm | Yêu cầu | Điểm yêu cầu | Code | Kiểm chứng runtime/rubric | Nơi đối chiếu |
| --- | --- | --- | --- | --- | --- | --- |
| 3 | Engineering Fundamentals | Có sử dụng Docker & Docker Compose | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `docker/docker-compose-*.yml` |
| 4 | Engineering Fundamentals | Optimize Dockerfile (ví dụ multistage build) | 3.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `docker/Dockerfile.airflow` |
| 5 | Implement Data Generator | Simulate skew | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/generator/batch_generator.py` |
| 6 | Implement Data Generator | Simulate schema evolution | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/generator/batch_generator.py` |
| 7 | Implement Data Generator | Simulate another offline data problem (Ví dụ: 2% duplicate rate) | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/generator/batch_generator.py` |
| 8 | Implement Data Generator | Store data after generating so that we can ingest to Bronze zone later | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/generator/batch_generator.py` |
| 9 | Implement Data Generator | Simulate late arrivals | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/generator/stream_generator.py` |
| 10 | Implement Data Generator | Simulate another streaming data problem (Ví dụ: 1.5% duplicate rate) | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/generator/stream_generator.py` |
| 11 | Processing Jobs | Spark Baseline (without optimization) | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/spark_baseline.py` |
| 12 | Processing Jobs | Spark Handle skew with explanation | 3.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/skew_experiment.py` |
| 13 | Processing Jobs | Spark Handle schema evolution with explanation | 3.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/spark_optimized.py` |
| 14 | Processing Jobs | Spark Handle other offline data problem with explanation | 3.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/spark_optimized.py` |
| 15 | Processing Jobs | Spark job được tích hợp vào các data pipeline | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `dags/dp1_raw_to_bronze.py` |
| 16 | Processing Jobs | Flink Baseline (without optimization) | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/flink/stream_baseline.py` |
| 17 | Processing Jobs | Flink Handle late arrival with explanation | 3.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/flink/stream_optimized.py` |
| 18 | Processing Jobs | Flink Handle other streaming problem with explanation | 3.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/flink/stream_optimized.py` |
| 19 | Processing Jobs | Flink Window processing | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/flink/stream_optimized.py` |
| 20 | Data Storage | Lakehouse (compaction, z-order, partitioning) | 3.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `scripts/optimize_storage.py` |
| 21 | Data Storage | Datawarehouse (indexing) | 3.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `scripts/setup_dwh_schemas.py` (indexing và benchmark) |
| 22 | Data Pipeline Orchestration | DP1 Ingest stage | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `dags/dp1_raw_to_bronze.py` |
| 23 | Data Pipeline Orchestration | DP1 Validate stage | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/spark_optimized.py` (validation trong DAG) |
| 24 | Data Pipeline Orchestration | DP2 Ingest stage | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `dags/dp2_bronze_to_silver_and_gold.py` |
| 25 | Data Pipeline Orchestration | DP2 Validate stage | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/spark_optimized.py` (validation trong DAG) |
| 26 | Data Pipeline Orchestration | DP3 Ingest stage | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `dags/dp3_compute_offline_features.py` |
| 27 | Data Pipeline Orchestration | DP3 Validate stage | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/spark_optimized.py` (validation trong DAG) |
| 28 | Data Governance | DP1 Lineage between pipeline and tables | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `governance/sync_catalog.py` |
| 29 | Data Governance | DP1 Data validation | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `governance/verify_contracts.py` |
| 30 | Data Governance | DP2 Lineage between pipeline and tables | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `governance/sync_catalog.py` |
| 31 | Data Governance | DP2 Data validation | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `governance/verify_contracts.py` |
| 32 | Data Governance | DP3 Lineage between pipeline and tables | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `governance/sync_catalog.py` |
| 33 | Data Governance | DP3 Data validation | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `governance/verify_contracts.py` |
| 34 | Documentation | Visualize tables on all zones | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/spark_optimized.py`, `scripts/setup_dwh_schemas.py` |
| 35 | Documentation | Dim table with SCD 2 (valid_from_ts, valid_to_ts, is_current) | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/spark_optimized.py` |
| 36 | Documentation | Feature tables (feat_ tables) with event_timestamp and created | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `feature_store/features.py` |
| 37 | Documentation | Relationship between dim & fact tables | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | `src/spark/spark_optimized.py`, `scripts/setup_dwh_schemas.py` |
| 38 | Documentation | Naming convention (raw_, stg_, dim_, fact_, feat_) | 2.0 | Có code; cần rà soát | Chưa kiểm chứng lại | Toàn bộ repo |

## Lượt hiện tại: dọn tài liệu và collectors

Đã loại bỏ report/checklist/evidence cũ và script chỉ đo/profile/kiểm tra chúng; cập nhật tham chiếu. Giữ code xử lý và hạ tầng. Đợt dọn trước: make test: 32 passed; make lint: pass. Đây là unit/lint checks, không cộng điểm runtime. Milestone tiếp theo: generator batch chọn dữ liệu; xem [LEARNING_ROADMAP](docs/LEARNING_ROADMAP.md).
