# CHI TIẾT LUỒNG DỮ LIỆU ĐẦU-CUỐI (END-TO-END DATA FLOW)
**Dự án:** E-Commerce Real-Time Purchase Propensity Prediction System (`ecom_ML_system`)  
**Khóa học:** EDAI K11 - Data Engineering (Mini-coursework)

---

## 1. SƠ ĐỒ CHUYỂN DỊCH DỮ LIỆU TỔNG QUAN

```text
               BATCH FLOW                                     STREAMING FLOW
     [2019-Oct.csv (01-25/10)]                          [2019-Oct.csv (26-31/10)]
                 │                                                  │
                 ▼                                                  ▼
       batch_generator.py                                  stream_generator.py
 (2% Duplicates, Schema Evolution)                   (Burst, Late 5-10m, Dup 1.5%)
                 │                                                  │
                 ▼                                                  ▼
     s3://ecommerce-raw/batch/                           Kafka Topic (key=user_id)
   - raw_events_old.csv (9 cols)                         ecommerce_stream_events
   - raw_events_new.csv (10 cols)                                   │
                 │                                                  ▼
                 │                                         Apache Flink 1.17
                 │                                    (Watermark 15m, HOP 15m/1m,
                 │                                     Deduplication ROW_NUMBER)
                 │                                                  │
                 │                              ┌───────────────────┴───────────────────┐
                 │                              ▼                                       ▼
                 │                   s3://ecommerce-raw/staging/            Kafka Topic
                 │                   stream_events/ (Append-only)    ecommerce_stream_features_15m
                 │                              │                                       │
                 └──────────────┬───────────────┘                                       ▼
                                ▼                                               Feast Stream Pusher
                        Spark Batch (DP1)                                               │
                                │                                                       ▼
                                ▼                                            Redis (Online Store)
                     Bronze: raw_events                                        Keys: [user_id]
                                │
                                ▼
                        Spark Batch (DP2)
             (Khử trùng lặp Spark dropDuplicates, Salting Skew Join)
                                │
                 ┌──────────────┴──────────────┐
                 ▼                             ▼
        Silver: stg_events                  Gold DWH
     (Làm sạch, thêm ingestion_time)   (Kimball Star Schema)
                 │                     - dim_product (SCD2)
                 │                     - dim_user (Snapshot)
                 │                     - fact_user_events (Pure Fact)
                 ▼
         Spark Batch (DP3)
   (Tính RFM 30 ngày & Nhãn 1h)
                 │
         ┌───────┴───────┐
         ▼               ▼
   feat_user_30d    user_labels
 (Feast Timestamps)   ([0, 1])
         │
         ▼
    Feast (DP4)
 (Materialization)
         │
         ▼
Redis (Online Store)
```

---

## 2. GIẢI THÍCH CHI TIẾT TỪNG CHẶNG TRUYỀN DỮ LIỆU

### Chặng 1: Cấp liệu thô & Tiêm lỗi thực nghiệm (Generation Stage)
1. **Luồng Batch (Offline):**
   * Đọc file `2019-Oct.csv` trong khoảng thời gian `01/10 → 25/10`.
   * **Tiêm Schema Evolution:** Nửa đầu (`01-15/10`) giữ nguyên 9 cột. Nửa sau (`16-25/10`) bổ sung cột thứ 10 `discount_percent`.
   * **Tiêm Duplicate:** Nhân bản ngẫu nhiên 2% số dòng và trộn đều vào tập dữ liệu.
   * Ghi 2 file CSV riêng biệt lên MinIO bucket `ecommerce-raw/batch/`.
2. **Luồng Streaming (Online):**
   * Đọc các ngày cuối tháng `26/10 → 31/10`.
   * Kế thừa đầy đủ 10 cột của Schema Evolution.
   * Bắn event thời gian thực vào Kafka topic `ecommerce_stream_events` với `key = user_id`.
   * Tiêm 3 lỗi: Flash sale burst x30 throughput, trễ mạng (ngâm sự kiện trong buffer 5-10 phút), và nhân bản 1.5% event do network retry.

---

### Chặng 2: Xử lý dòng Flink & Phân luồng Staging (Stream Processing)
1. **Tiếp nhận & Watermark:** Flink đọc Kafka và áp dụng Watermark lùi 15 phút so với Event Time (`WATERMARK FOR row_time AS row_time - INTERVAL '15' MINUTE`). Thiết kế này nhằm bao dung các sự kiện đến trễ trong khoảng 5-10 phút như kịch bản mô phỏng.
2. **Khử trùng lặp dòng:** Áp dụng câu lệnh cửa sổ:
   ```sql
   ROW_NUMBER() OVER (PARTITION BY user_id, event_time, product_id, event_type ORDER BY row_time ASC) = 1
   ```
   Các bản ghi trùng lặp chia sẻ cùng bộ khóa cấu hình được quy giảm về bản ghi xuất hiện đầu tiên theo `row_time`.
3. **Phân nhánh đầu ra (Kiến trúc phân tách Raw Audit vs Realtime Features):**
   ```text
   Kafka Topic (ecommerce_stream_events)
    │
    ├─► [Nhánh Raw Staging]: Ghi stream thô nguyên trạng (Append-only) ──► s3://ecommerce-raw/staging/stream_events/
    │                                                                           │
    │                                                                           ▼ (DP1 Batch Ingestion)
    │                                                                   Bronze: raw_events
    │                                                                           │
    │                                                                           ▼ (DP2 Spark Batch dropDuplicates)
    │                                                                   Silver: stg_events (Khử trùng lặp)
    │
    └─► [Nhánh Real-time Feature]: Khử trùng lặp (ROW_NUMBER=1)
            │
            ▼
        Hopping Window (15m window, 1m slide)
            │
            ▼
        Kafka Topic (ecommerce_stream_features_15m) ──► Feast Stream Pusher ──► Redis Online Store
   ```
   * **Nhánh Feature:** Đọc từ view đã khử trùng lặp (`deduped_stream_events`), gom vào cửa sổ trượt (Hopping Window) 15 phút (trượt mỗi 1 phút: `HOP(row_time, INTERVAL '1' MINUTE, INTERVAL '15' MINUTE)`), tính 4 đặc trưng người dùng và phát vào Kafka topic `ecommerce_stream_features_15m`.
   * **Nhánh Staging MinIO:** Streaming staging preserves raw events for replay/audit. Flink ghi nhận stream thô nguyên trạng vào `s3://ecommerce-raw/staging/stream_events/`. Deduplication is applied downstream for Silver processing and/or realtime feature computation. Cụ thể, việc khử trùng lặp cho nhánh Lakehouse được chuyển giao cho Spark ở tầng Silver downstream (`dropDuplicates`).

---

### Chặng 3: Data Lakehouse Ingestion & Medallion Architecture (Spark Batch)
1. **DP1: Raw → Bronze Lakehouse:**
   * Spark đọc đồng thời Batch CSV cũ (9 cột), mới (10 cột) và Flink Staging JSON.
   * Kích hoạt thuộc tính `.option("mergeSchema", "true")`. Delta Lake tự động mở rộng schema nhận cột `discount_percent` (các dòng cũ tự động mang giá trị `NULL`).
   * Bổ sung cột siêu dữ liệu chuẩn hóa: `ingestion_time` (thời điểm nạp vào Bronze).
   * Lưu trữ phân vùng theo `date` tại `s3a://ecommerce-lakehouse/bronze/raw_events/`.
2. **DP2: Bronze → Silver & Gold DWH:**
   * **Tầng Silver (`stg_events`):** Thực hiện `dropDuplicates(["user_id", "event_time", "product_id", "event_type"])` để khử trùng lặp từ Batch và Streaming Staging. Điền giá trị mặc định cho `discount_percent = 0.0`.
   * **Xử lý Data Skew:** Do ngành hàng `electronics` chiếm tỷ trọng lớn, Spark áp dụng Salting key (chia 4 nhánh ngẫu nhiên) kết hợp `F.broadcast()` khi join với danh mục, triệt tiêu hiện tượng lệch tải partition.
   * **Xây dựng Gold DWH (Kimball Star Schema):**
     * `dim_product`: **SCD Type 2 lịch sử** dựa trên các trạng thái thuộc tính quan sát được với `valid_from_ts`, `valid_to_ts = lead(valid_from_ts)`, `is_current`, và surrogate key `product_sk`.
     * `dim_user`: Bảng **Snapshot / Current-state dimension** lưu trạng thái hiện tại của người dùng (`first_seen`, `last_seen`, `total_lifetime_events`, `is_active`, `valid_from_ts`, `valid_to_ts`, `is_current`).
     * `fact_user_events`: Pure Fact Table chứa `product_sk`, không chứa `product_id`.
3. **DP3: Gold DWH → Feature Store & Labels:**
   * Spark tính toán các chỉ số RFM (Recency, Frequency, Monetary) trong 30 ngày cho `feat_user_30d`.
   * Gán 2 cột chuẩn hóa Feast: `event_timestamp` (mốc thời điểm tính toán) và `created` (thời điểm tạo bản ghi).
   * Tạo bảng nhãn nhị phân `user_labels`: `target_purchase_1h = 1` nếu khách hàng mua hàng trong vòng 1 giờ tiếp theo, ngược lại bằng `0`.

---

### Chặng 4: Phục vụ mô hình thời gian thực (Feast & Redis Online Serving)
1. **Materialization (DP4):** Airflow kích hoạt Python script thực thi `feast materialize`, kéo dữ liệu đặc trưng lịch sử mới nhất từ Delta Lake nạp vào Redis Online Store container (`localhost:6379`).
2. **Stream Feature Push:** Tiến trình Python độc lập `feature_store/stream_push_job.py` lắng nghe Kafka topic `ecommerce_stream_features_15m` và đẩy các vector đặc trưng 15m mới tính vào Redis và lưu trữ file parquet offline.
3. **Online Inference Serving:** Mô hình Machine Learning truy vấn Redis qua `user_id` để lấy kết hợp cả đặc trưng 30 ngày (Batch) và 15 phút (Streaming) với độ trễ `< 5ms`, đảm bảo hoàn toàn không bị Train-Serve Skew hay Data Leakage.

---

### Chặng 5: Quản trị siêu dữ liệu & Kiểm định chất lượng (DataHub Governance)
1. **Đồng bộ Catalog & Lineage:** `sync_catalog.py` gửi toàn bộ thông tin mô tả 12 datasets và sơ đồ phả hệ (mũi tên liên kết cha-con) sang DataHub GMS (Host: `http://localhost:8089`, Docker: `http://datahub-gms:8080`) qua REST API.
2. **Kiểm định Hợp đồng (Data Contracts):** `verify_contracts.py` dùng Delta Lake snapshot / PyArrow đọc kiểm định trên mẫu 50,000 dòng thực tế của các bảng trên MinIO, kiểm tra 0 NULL, 0 Duplicate, toàn vẹn khóa ngoại và khoảng thời gian hợp lệ SCD2 (`valid_from_ts <= valid_to_ts`), sau đó gửi kết quả `AssertionRunEvent` lên DataHub GMS để quan sát trực tiếp trên DataHub UI (`http://localhost:9002`).
