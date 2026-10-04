# QUẢN TRỊ DỮ LIỆU TOÀN DIỆN VỚI DATAHUB (DATAHUB GOVERNANCE GUIDE)
**Dự án:** E-Commerce Real-Time Purchase Propensity Prediction System (`ecom_ML_system`)  
**Khóa học:** EDAI K11 - Data Engineering (Mini-coursework)

---

## 1. DATAHUB LÀ GÌ? (WHAT DATAHUB IS)
* **Khái niệm:** DataHub là một nền tảng quản trị và khám phá siêu dữ liệu doanh nghiệp (Metadata Platform) mã nguồn mở được phát triển bởi LinkedIn.
* **Vai trò trong hệ thống:** DataHub đóng vai trò là "cuốn danh bạ và đài quan sát", lưu trữ toàn bộ thông tin về các bảng dữ liệu, cấu trúc cột, người sở hữu (Owner), mức độ tin cậy (Quality), và phả hệ luồng đi của dữ liệu (Lineage).
* **Nguyên lý quan trọng:** DataHub **không** nằm trên đường đi thực tế của dữ liệu (Data Plane). Nó không xử lý event thay Kafka/Spark và không lưu trữ dữ liệu kinh doanh. DataHub chỉ lưu trữ các thông tin mô tả về dữ liệu (Metadata).

---

## 2. METADATA LÀ GÌ? (WHAT METADATA IS)
* **Định nghĩa:** Metadata là "dữ liệu về dữ liệu" (Data about data).
* **Trong dự án này, Metadata bao gồm:**
  * **Cấu trúc bảng (Schema):** Tên trường, kiểu dữ liệu (`string`, `number`, `time`), mô tả nghiệp vụ, trường khóa chính (Primary Key).
  * **Quyền sở hữu (Ownership):** Bảng này do kỹ sư nào phụ trách (`urn:li:corpuser:data_engineer` hay `mlops_engineer`).
  * **Gắn nhãn phân loại (Tags):** `Bronze`, `Silver`, `Gold`, `SCD2`, `Feast`, `Append_Only`.
  * **Phân vùng miền (Domains):** `bronze_lakehouse`, `silver_lakehouse`, `gold_dwh`, `feature_store`.
  * **Phả hệ (Lineage):** Bảng này được sinh ra từ bảng nào ở thượng nguồn (Upstream).
  * **Hợp đồng & Kiểm định (Data Contracts & Assertions):** Quy chuẩn chất lượng (không được có NULL, không được trùng lặp).

---

## 3. CÁC BẢNG DỮ LIỆU ĐÃ ĐĂNG KÝ (REGISTERED DATASETS)

Hệ thống khai báo tập trung và sẵn sàng đồng bộ **12 Datasets** đại diện cho toàn bộ kiến trúc Medallion Lakehouse và Feature Store:

| STT | Dataset Key | Tên thực tế trong hệ thống | Nền tảng (Platform) | URN trên DataHub | Vai trò kiến trúc |
|:---:|:---|:---|:---:|:---|:---|
| 1 | `raw_batch` | `ecommerce-raw/batch` | S3 (MinIO) | `urn:li:dataset:(urn:li:dataPlatform:s3,ecommerce-raw/batch,PROD)` | Nguồn CSV lịch sử (Batch Source) |
| 2 | `raw_stream` | `ecommerce_stream_events` | Kafka | `urn:li:dataset:(urn:li:dataPlatform:kafka,ecommerce_stream_events,PROD)` | Nguồn sự kiện trực tuyến (Stream Source) |
| 3 | `raw_staging_stream` | `ecommerce-raw/staging/stream_events` | S3 (MinIO) | `urn:li:dataset:(urn:li:dataPlatform:s3,ecommerce-raw/staging/stream_events,PROD)` | MinIO Stream Staging (JSON do Flink ghi) |
| 4 | `bronze_raw_events` | `ecommerce-lakehouse/bronze/raw_events` | Delta Lake | `urn:li:dataset:(urn:li:dataPlatform:delta,ecommerce-lakehouse/bronze/raw_events,PROD)` | Tầng Bronze (Raw truth Delta Lake) |
| 5 | `silver_stg_events` | `ecommerce-lakehouse/silver/stg_events` | Delta Lake | `urn:li:dataset:(urn:li:dataPlatform:delta,ecommerce-lakehouse/silver/stg_events,PROD)` | Tầng Silver (Cleaned, Deduped & Partitioned) |
| 6 | `gold_dim_product` | `ecommerce-lakehouse/gold/dim_product` | Delta Lake | `urn:li:dataset:(urn:li:dataPlatform:delta,ecommerce-lakehouse/gold/dim_product,PROD)` | Bảng chiều sản phẩm (SCD-like Type 2, category_level1) |
| 7 | `gold_dim_user` | `ecommerce-lakehouse/gold/dim_user` | Delta Lake | `urn:li:dataset:(urn:li:dataPlatform:delta,ecommerce-lakehouse/gold/dim_user,PROD)` | Bảng chiều người dùng (Current-state Snapshot Dimension) |
| 8 | `gold_fact_user_events` | `ecommerce-lakehouse/gold/fact_user_events` | Delta Lake | `urn:li:dataset:(urn:li:dataPlatform:delta,ecommerce-lakehouse/gold/fact_user_events,PROD)` | Bảng sự kiện thuần túy (Pure Fact Table, không có product_id) |
| 9 | `gold_feat_user_30d` | `ecommerce-lakehouse/gold/feat_user_30d` | Delta Lake | `urn:li:dataset:(urn:li:dataPlatform:delta,ecommerce-lakehouse/gold/feat_user_30d,PROD)` | Bảng đặc trưng 30 ngày (Batch Features chuẩn Feast, partition date) |
| 10 | `gold_feat_user_stream` | `ecommerce-lakehouse/gold/feat_user_stream/stream_features.parquet` | S3 (MinIO) | `urn:li:dataset:(urn:li:dataPlatform:s3,ecommerce-lakehouse/gold/feat_user_stream/stream_features.parquet,PROD)` | Tệp Parquet đặc trưng 15 phút (Feast FileSource dual-write) |
| 11 | `gold_user_labels` | `ecommerce-lakehouse/gold/user_labels` | Delta Lake | `urn:li:dataset:(urn:li:dataPlatform:delta,ecommerce-lakehouse/gold/user_labels,PROD)` | Bảng nhãn học máy (ML Ground Truth [0, 1], partition date) |
| 12 | `online_redis` | `ecommerce-feature-store/online_redis` | Redis | `urn:li:dataset:(urn:li:dataPlatform:redis,ecommerce-feature-store/online_redis,PROD)` | Kho phục vụ trực tuyến thời gian thực (Online Serving) |

---

## 4. CÁCH ĐĂNG KÝ CẤU TRÚC BẢNG (SCHEMA REGISTRATION)
* **Khai báo tập trung (Declarative Catalog):** Toàn bộ schema được khai báo bằng Python code trong file `governance/catalog.py` bằng cấu trúc dữ liệu `SchemaFieldSpec(field_path, type, description)`.
* **Khớp chuẩn xác với Runtime Spark/Flink:**
  * `dim_product`: Khai báo chính xác các cột thực tế: `product_sk`, `product_id`, `category_id`, `category_level1`, `brand`, `price`, `discount_percent`, `valid_from_ts`, `valid_to_ts`, `is_current` (tuyệt đối không khai báo `category_code`).
  * `dim_user`: Khai báo chính xác các cột snapshot: `user_id`, `first_seen`, `last_seen`, `total_lifetime_events`, `is_active`, `valid_from_ts`, `valid_to_ts`, `is_current` (không dùng schema cũ `total_events`, `total_spend`).
  * `fact_user_events`: Khai báo đầy đủ 12 cột: `event_id`, `event_timestamp`, `event_time`, `date`, `user_id`, `product_sk`, `category_level1`, `brand`, `price`, `discount_percent`, `event_type`, `user_session` (không chứa `product_id`).
  * `feat_user_30d` & `user_labels`: Khai báo đầy đủ cột phân vùng vật lý `date` và các cột chuẩn thời gian Feast (`event_timestamp`, `created`).
* **Cơ chế phát:** `governance/sync_catalog.py` chuyển đổi các khai báo này thành gói tin `SchemaMetadataClass` và gửi qua REST API đến DataHub GMS.

---

## 5. CÁCH BIỂU DIỄN PHẢ HỆ (LINEAGE REPRESENTATION)
Phả hệ dữ liệu trên DataHub được thiết lập ở 2 cấp độ:

```text
[Kafka Stream Topic] ──► (Flink Staging) ──► [MinIO Stream Staging] ──┐
                                                                      ├──► (DP1 Ingest) ──► [Bronze Delta]
[S3 Batch CSV]       ─────────────────────────────────────────────────┘                         │
                                                                                                ▼ (DP2 Ingest)
                                                                                         [Silver Delta]
                                                                                                │
                                                    ┌───────────────────────────────────────────┼───────────────────────────────────────────┐
                                                    ▼ (DP2 Modeling)                            ▼ (DP2 Modeling)                            ▼ (DP2 Modeling)
                                           [Gold dim_product (SCD2)]                  [Gold dim_user (Snapshot)]                   [Gold fact_user_events]
                                                                                                │
                                                                                                ▼ (DP3 Features & Labels)
                                                                                    ┌───────────┴───────────┐
                                                                                    ▼                       ▼
                                                                           [Gold feat_user_30d]     [Gold user_labels]
                                                                                    │
                                                                                    ▼ (DP4 Materialize)
                                                                           [Redis Online Store] ◄── [Stream Pusher] ◄── [Kafka Stream Topic]
                                                                                                        │
                                                                                                        ▼ (Dual-Write)
                                                                                           [Gold feat_user_stream Parquet]
```

1. **Dataset-level Lineage (`UpstreamLineageClass`):** Khai báo trực tiếp quan hệ cha-con giữa các bảng (ví dụ: `Bronze` nhận cả `raw_batch` và `raw_staging_stream` làm thượng nguồn; `Silver` nhận `Bronze`; `Gold Fact` nhận `Silver`, `dim_product`, `dim_user`).
2. **Task-level Lineage (`DataFlowInfoClass` & `DataJobInputOutputClass`):** Khai báo các Pipeline Airflow DP1, DP2, DP3 dưới dạng DataFlow và các task `ingest_stage`, `validate_stage` dưới dạng DataJob với tập đầu vào (`inputDatasets`) và đầu ra (`outputDatasets`) chuẩn xác.

---

## 6. HỢP ĐỒNG DỮ LIỆU & KIỂM ĐỊNH (DATA CONTRACTS & ASSERTIONS)
Mỗi tầng dữ liệu có một bản cam kết chất lượng (Data Contract) với các chỉ số kiểm định (Assertions) tự động:
1. **Bronze Contract (`contract_bronze_lakehouse`):**
   * Assertion `dp1_bronze_null_check`: Cam kết 0 NULL ở cột `user_id`.
2. **Silver Contract (`contract_silver_curated`):**
   * Assertion `dp2_silver_dedup_check`: Cam kết tính duy nhất trên tập deduplication key (0 bản ghi trùng lặp).
3. **Gold DWH Contract (`contract_gold_dim_product`):**
   * Assertion `dp2_scd2_integrity_check`: Cam kết toàn vẹn khóa thay thế `product_sk` (0 NULL) VÀ tính hợp lệ khoảng thời gian `valid_from_ts <= valid_to_ts` trên các bản ghi lịch sử.
4. **Gold Feature Contract (`contract_feat_user_30d`):**
   * Assertion `dp3_feast_schema_contract`: Cam kết tồn tại và không NULL ở 2 cột chuẩn Feast (`event_timestamp` và `created`).
5. **Gold Labels Contract (`contract_user_labels`):**
   * Assertion `dp3_binary_labels_contract`: Cam kết nhãn `target_purchase_1h` thuộc tập nhị phân `[0, 1]` trên tập mẫu kiểm định.

---

## 7. CƠ CHẾ METADATA ĐẾN ĐƯỢC DATAHUB (HOW METADATA REACHES DATAHUB)
Hệ thống sử dụng cơ chế **REST API Metadata Change Proposals (MCPs)**:
1. Python SDK `datahub` khởi tạo `DatahubRestEmitter(gms_url)`.
2. **Quy tắc phân định URL bắt buộc:**
   * **Trên Máy Chủ (Host Access):**
     * DataHub GMS REST API: `http://localhost:8089` (cổng 8080 của container được ánh xạ ra 8089).
     * DataHub Web UI: `http://localhost:9002` (giao diện đồ họa người dùng).
   * **Trong Mạng Docker (Container-to-Container):**
     * DataHub GMS: `http://datahub-gms:8080` (sử dụng hostname nội bộ Docker).
     * Tuyệt đối không dùng `localhost` khi các container gọi nhau qua mạng.
3. Emitter gửi các gói tin HTTP POST chứa đối tượng Aspect (Schema, Lineage, DataFlow, DataJob, AssertionResult) sang DataHub GMS.
4. DataHub GMS xử lý và lưu trữ vào OpenSearch (tìm kiếm) và MySQL (lưu trữ quan hệ).

---

## 8. TÍNH CHẤT TÍCH HỢP AIRFLOW: DECLARATIVE SYNC (OPTION B)
* **Quyết định kiến trúc:** Hệ thống áp dụng **Option B (Declarative Governance via Metadata-as-Code)**.
* **Chi tiết kỹ thuật:**
  * Siêu dữ liệu về DataFlows và DataJobs (DP1, DP2, DP3) cùng Dataset Lineage được khai báo và đồng bộ tập trung qua `governance/catalog.py` và `governance/sync_catalog.py`.
  * Không kích hoạt plugin listener tự động runtime (`acryl-datahub-airflow-plugin`) nhằm đảm bảo tính độc lập tuyệt đối giữa các container, loại bỏ nguy cơ xung đột phiên bản thư viện với Airflow 2.7.3 và giữ cho scheduler luôn ổn định.
  * Tài liệu ghi nhận minh bạch: *DataHub lineage và metadata được đăng ký tường minh (declarative), không phát sinh tự động từ Airflow runtime listener.*

---

## 9. PHÂN BIỆT SAMPLE-BASED VS FULL VALIDATION
* **Thực trạng kiểm định:** File `governance/verify_contracts.py` thực hiện kiểm định chất lượng dữ liệu trên mẫu:
  ```python
  df = tbl.slice(0, 50000).to_pandas()
  ```
* **Tại sao dùng kiểm định mẫu (Sample-based)?**
  * Dữ liệu Delta Lake quy mô lớn hàng triệu bản ghi không nên tải toàn bộ vào RAM bằng Pandas để kiểm tra cục bộ vì dễ gây tràn bộ nhớ (Out-Of-Memory).
  * Lấy mẫu 50,000 dòng là kỹ thuật kiểm tra mẫu kiểm toán (Statistical Auditing) chuẩn công nghiệp, cung cấp phản hồi nhanh (dưới 5 giây) cho việc phát hiện sớm bất thường dữ liệu.
* **Minh bạch hóa:** Báo cáo ghi nhận đúng đây là **Sample-based data quality validation**, không tuyên bố là quét toàn bộ 100% bảng vật lý.

---

## 10. HƯỚNG DẪN XÁC MINH TRÊN DATAHUB
1. **Đồng bộ siêu dữ liệu:**
   ```bash
   python governance/sync_catalog.py --gms-url http://localhost:8089
   ```
2. **Kiểm định chất lượng dữ liệu và phát kết quả Assertion:**
   ```bash
   python governance/verify_contracts.py --gms-url http://localhost:8089
   ```
3. **Mở giao diện Web UI:**
   Truy cập `http://localhost:9002` (user: `datahub`, pass: `datahub`), kiểm tra Lineage đồ thị và tab Quality của các Datasets.
