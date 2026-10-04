# Hệ Thống Mục Lục Tài Liệu Kỹ Thuật (Docs Index)

> Bảng tra cứu tập trung toàn bộ tài liệu kỹ thuật trong thư mục `docs/`, sắp xếp theo thứ tự rubric đồ án EDAI K11 DE (`rubic/EDAI K11 - DE.xlsx`, sheet `edai-1`).

| STT | Tài liệu kỹ thuật | Mục đích nghiệp vụ & Kỹ thuật | Hạng mục Rubric chứng minh | Điểm Rubric |
| :-: | :--- | :--- | :--- | :-: |
| 1 | [`Docker_Optimization.md`](Docker_Optimization.md) | Báo cáo tối ưu Dockerfile: Baseline (5.47GB) vs Multistage (3.52GB) | Engineering Fundamentals: Dockerfile | 5.0đ |
| 2 | [`Data_Generator.md`](Data_Generator.md) | Đặc tả bộ sinh dữ liệu Batch & Stream, tiêm lỗi Skew, Cardinality, Evolution, Duplicate, Late Arrival | Implement Data Generator | 12.0đ |
| 3 | [`Spark_Baseline_Report.md`](Spark_Baseline_Report.md) | Báo cáo thực nghiệm Spark Baseline tắt AQE, bộc lộ task straggler và skew | Processing Jobs - Spark: Baseline | 2.0đ |
| 4 | [`Spark_Optimization_Report.md`](Spark_Optimization_Report.md) | Báo cáo tối ưu Spark: AQE Skew Join, Salting, Schema Evolution, Deduplication, Airflow Integration | Processing Jobs - Spark: Optimization | 11.0đ |
| 5 | [`Flink_Baseline_Report.md`](Flink_Baseline_Report.md) | Báo cáo thực nghiệm Flink Baseline đơn luồng, bộc lộ Backpressure khi burst traffic | Processing Jobs - Flink: Baseline | 2.0đ |
| 6 | [`Flink_Optimized_Report.md`](Flink_Optimized_Report.md) | Báo cáo tối ưu Flink: Parallelism=3, Buffer Debloating, Watermark 15m, Top-1 Dedup, Hopping Window | Processing Jobs - Flink: Optimization | 8.0đ |
| 7 | [`Data_Storage_Optimization.md`](Data_Storage_Optimization.md) | Báo cáo tối ưu Lakehouse (Compaction, Z-Order, Vacuum an toàn) và DWH B-Tree Indexing (148x) | Data Storage: Lakehouse & DWH | 6.0đ |
| 8 | [`Airflow_Orchestration_Report.md`](Airflow_Orchestration_Report.md) | Báo cáo điều phối tự động 4 DAGs DP1 -> DP4 theo chuỗi Ingest >> Validate >> Trigger Downstream | Data Pipeline Orchestration | 12.0đ |
| 9 | [`Data_Governance_Report.md`](Data_Governance_Report.md) | Báo cáo quản trị siêu dữ liệu: 12 Datasets, Lineage đồ thị đa tầng và kiểm định Data Contracts trên DataHub | Data Governance: Lineage & Validation | 12.0đ |
| 10 | [`Schema_Design.md`](Schema_Design.md) | Thiết kế Medallion Architecture 4 zones, Star Schema DWH, dim_product SCD Type 2, và bảng feat_ | Documentation: Schema Design | 10.0đ |
| 11 | [`Feature_Store_TTL_Report.md`](Feature_Store_TTL_Report.md) | Báo cáo kiến trúc Feast Feature Store, luận chứng TTL 30 ngày (Batch) vs 2 giờ (Stream), serving < 2ms | Feature Store & Serving | Phụ trợ |
| 12 | [`TECH_STACK.md`](TECH_STACK.md) | Bảng tổng hợp công nghệ, phiên bản, URL các cổng dịch vụ và căn cứ lựa chọn kiến trúc | Tổng quan công nghệ | Phụ trợ |
| 13 | [`DATA_FLOW.md`](DATA_FLOW.md) | Sơ đồ luân chuyển dữ liệu End-to-End và mốc thời gian dữ liệu (Batch, Stream, As-of Date) | Luồng dữ liệu hệ thống | Phụ trợ |
| 14 | [`ARCHITECTURE.md`](ARCHITECTURE.md) | Kiến trúc hệ thống tổng thể, phân rã các deployable units theo chuẩn rubric | Kiến trúc hệ thống | Phụ trợ |
| 15 | [`RUNBOOK.md`](RUNBOOK.md) | Hướng dẫn vận hành chi tiết từng bước từ trạng thái sạch và quy trình chụp minh chứng E01-E30 | Quy trình vận hành & Tái lập | Vận hành |
| 16 | [`EVIDENCE_TODO.md`](EVIDENCE_TODO.md) | Bảng kiểm tra danh sách 30 ảnh chụp minh chứng giao diện UI phục vụ đánh giá | Checklist minh chứng | Đánh giá |

---

## Thư Mục Bằng Chứng Định Lượng (`docs/evidence/`)
- `docker_sizes.txt`: Đo kích thước 3 biến thể Docker Image qua Docker Engine.
- `data_profile.md` / `.json`: Hồ sơ 5 đặc tính dữ liệu sinh ra (khớp 100% manifest).
- `scale_cardinality.md`: Thống kê độ nở của user_id cardinality theo số lượng replica.
- `spark_skew_experiment.md`: Benchmark 3 biến thể xử lý Data Skew (Baseline, AQE, Salting).
- `stream_profile.md`: Thống kê lưu lượng streaming, tỉ lệ late arrival và duplicate.
- `lakehouse_inspection.txt`: Thống kê số bản ghi các bảng Lakehouse và compaction.
- `dwh_explain_analyze.txt`: Kế hoạch thực thi EXPLAIN (ANALYZE, BUFFERS) trước và sau đánh index.
- `feast_incremental.txt`: Benchmark cơ chế incremental materialize và độ trễ online serving.
- `coverage.txt`: Độ phủ kiểm thử tự động đo lường thực tế trên mã nguồn.
