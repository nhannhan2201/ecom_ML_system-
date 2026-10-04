# Luồng dữ liệu hiện có trong source

## File này dùng để làm gì

Đây là bản đồ **hiện trạng source**: entrypoint, input/output và consumer tiếp theo như được nối trong code/config/DAG. Nó trả lời “repo hiện đang ghép các phần ra sao?”, không phải “ta muốn xây hệ thống thế nào?” (xem [WorkFlow.md](WorkFlow.md)) hay “ta đã học tới đâu?” (xem [LEARNING_ROADMAP.md](LEARNING_ROADMAP.md)).

Kết nối có trong source không đồng nghĩa job đã chạy đúng. Mỗi cạnh phải được hiểu là một trong hai trạng thái: static connection inspected, hoặc runtime verified với lệnh và kết quả. Phạm vi bản đồ hiện có cần được đọc lại theo component trước khi dùng làm bằng chứng.

Thiết kế mục tiêu là bốn feature 15 phút; xem [WorkFlow.md](WorkFlow.md). Source hiện tại vẫn có feature 30 ngày. Bản đồ này chỉ mô tả kết nối hiện có theo code, không xác nhận runtime.

## Producer → nơi lưu → consumer hiện có trong source

Sơ đồ dưới đây chỉ mô tả đường đi hiện tại xác nhận được từ lời gọi/đích trong code. Mũi tên không khẳng định job đã chạy thành công.

```mermaid
flowchart LR
  OCT["CSV Oct"] --> BG["Batch generator\\nsrc/generator/batch_generator.py"] --> RAW["MinIO raw batch"] --> DP1["Spark DP1\\nsrc/spark/spark_optimized.py"] --> BR["Bronze Delta"]
  CSVN["CSV input stream"] --> SG["Stream generator\\nsrc/generator/stream_generator.py"] --> K["Kafka events"] --> FL["Flink optimized\\nsrc/flink/stream_optimized.py"]
  FL --> ST["MinIO staging JSON"]
  FL --> FT["Kafka feature topic"] --> PUSH["Feast pusher\\nfeature_store/stream_push_job.py"] --> REDIS["Redis"]
  BR --> DP2["Spark DP2"] --> SILVER["Silver + Gold Delta"] --> DP3["Spark DP3"] --> F30["30d features + labels"]
  F30 --> MAT["Feast materialize DAG/job"] --> REDIS
  SILVER --> DWH["DWH setup/sync\\nscripts/setup_dwh_schemas.py"] --> PG["PostgreSQL"]
```

| Producer | Output mặc định | Consumer |
| --- | --- | --- |
| [Batch generator](src/generator/batch_generator.py) đọc 2019-Oct.csv | MinIO ecommerce-raw/batch/raw_events_old*.csv và raw_events_new*.csv; small còn lưu local | Spark DP1 |
| [Stream generator](src/generator/stream_generator.py) replay CSV | Kafka ecommerce_stream_events, JSON, key=user_id | Flink |
| [Flink optimized](src/flink/stream_optimized.py) nhánh raw | MinIO ecommerce-raw/staging/stream_events/ JSON | Spark DP1 |
| Flink nhánh dedup/window | Kafka ecommerce_stream_features_15m | Feast stream pusher |
| [Spark DP1](src/spark/spark_optimized.py) | Bronze Delta raw_events | DP2 |
| Spark DP2 | Silver stg_events, Gold dim_product/dim_user/fact_user_events Delta trên MinIO | DP3 đọc Silver; script DWH sync đọc Lakehouse |
| Spark DP3 | feat_user_30d/user_labels Delta; Parquet export feast/user_batch_features_30d | Feast batch materialize và historical retrieval |
| [DWH sync](scripts/setup_dwh_schemas.py) | PostgreSQL bronze/silver/gold | SQL analytics |
| [Feast materialize](feature_store/materialize.py) | Redis batch features | Online retrieval |
| [Stream pusher](feature_store/stream_push_job.py) | Redis mặc định; offline/both là tùy chọn cần kiểm tra thực tế | Online/historical retrieval |

Airflow DAGs gọi Spark stages/validation và trigger downstream theo định nghĩa DAG. DataHub catalog/lineage được khai báo và gửi qua scripts riêng. Cả hai kết nối này mới là xác nhận từ source.

Thiết kế 15m-only, feature/prediction contract và luồng mong muốn nằm riêng ở [WorkFlow.md](WorkFlow.md); sơ đồ đó không được trộn vào sơ đồ code hiện trạng này.

## Những điểm cần kiểm tra khi tới component

- Batch small chọn đầu CSV và skip dòng cố định, chưa lọc date_range. Schema/fault flags cần đối chiếu với logic thực.
- Stream late buffer dùng event time; enqueue counter không phải Kafka ACK. Checkpoint chưa chứng minh resume không mất/lặp.
- DP1 append staging rồi archive không atomic; retry và bootstrap dở dang cần fixture.
- Silver dedup theo user_id/event_time/product_id/event_type; event_id Gold lại bỏ event_type. SCD2 cần thử giá A→B→A.
- Một số scripts đọc thư mục Parquet thay vì active snapshot Delta; cần đối chiếu trước khi tin count hoặc nạp DWH.
- DP4 kiểm thư mục Gold, còn Feast đọc Parquet export riêng. Pusher auto-commit và flush cuối phiên cần kiểm chứng.

Khi debug: chọn một khóa event → tìm ở nguồn → Bronze/staging → Silver/window → fact/feature → consumer. Kiểm timestamp, count và checkpoint ở mỗi chặng; không suy toàn luồng đúng từ port mở, tên bảng hoặc log PASS.
