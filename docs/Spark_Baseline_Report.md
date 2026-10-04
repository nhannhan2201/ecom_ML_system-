# BÁO CÁO SPARK BATCH PROCESSING BASELINE (CHƯA TỐI ƯU)
**Dự án:** E-Commerce Real-Time Purchase Propensity Prediction System  
**Học phần:** Mini-Coursework: Spark job to handle offline data problems  
**Điểm Rubric mục tiêu:** 2.0 / 16.0 điểm (Baseline without optimization)  
**File mã nguồn thực thi:** [`src/spark/spark_baseline.py`](../src/spark/spark_baseline.py)  

---

## 1. MỤC TIÊU & TRIẾT LÝ THIẾT KẾ BẢN BASELINE

Bản **Baseline** được thiết kế nhằm mô phỏng mã nguồn của một Data Engineer chạy xử lý hàng loạt (Batch Processing) theo cách "ngây thơ" (Naive / Unoptimized):
- Chưa am hiểu kiến trúc phân tán của Spark, dẫn tới việc giữ nguyên các cấu hình mặc định kém hiệu quả.
- Tắt các cơ chế tối ưu hóa hiện đại của Spark 3.x (Adaptive Query Execution, Skew Join, Broadcast Join).
- Bộc lộ rõ nét **4 vấn đề lớn của dữ liệu offline** theo đúng yêu cầu của Rubric:
  1. **Data Skew:** Gây hiện tượng Task Straggler làm nghẽn Stage.
  2. **High Cardinality:** Gây tràn bộ nhớ Heap và Shuffle Spill ra Disk.
  3. **Schema Evolution:** Đọc nối CSV thô khi schema thay đổi cấu trúc cột.
  4. **Offline Duplicates:** Bỏ sót 2% bản ghi trùng lặp rác lọt vào kho dữ liệu.

---

## 2. CẤU HÌNH SPARK BASELINE (ANTI-PATTERNS)

Trong [`src/spark/spark_baseline.py`](../src/spark/spark_baseline.py), SparkSession được cố tình thiết lập các Anti-Pattern sau:

```python
spark = (
    SparkSession.builder
    .appName("ECom-Spark-Offline-Baseline")
    .master("local[*]")
    # [ANTI-PATTERN 1]: Tắt toàn bộ Adaptive Query Execution (AQE)
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.adaptive.skewJoin.enabled", "false")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
    
    # [ANTI-PATTERN 2]: Tắt Broadcast Join (Ép buộc dùng Shuffle Hash Join / Sort Merge Join)
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    # [ANTI-PATTERN 3]: Số partition shuffle cố định mặc định 200 (không co giãn)
    .config("spark.sql.shuffle.partitions", "200")
    
    # [ANTI-PATTERN 4]: Giới hạn RAM Executor ở mức thấp (1GB) để phơi bày Shuffle Spill
    .config("spark.executor.memory", "1g")
    .config("spark.driver.memory", "1g")
    .getOrCreate()
)
```

---

## 3. PHÂN TÍCH CHI TIẾT 4 VẤN ĐỀ DỮ LIỆU OFFLINE TRÊN THỰC TẾ

### 3.1. Vấn đề 1: Schema Evolution (Thay đổi cấu trúc bảng theo thời gian)
* **Thực trạng dữ liệu:**
  - Tập `raw_events_old.csv` (01/10 → 15/10) gồm **9 cột**: `[brand, category_code, category_id, event_time, event_type, price, product_id, user_id, user_session]`.
  - Tập `raw_events_new.csv` (16/10 → 25/10) gồm **10 cột**: Bổ sung thêm cột khuyến mãi `discount_percent`.
* **Cách Baseline xử lý:**
  - Đọc bằng hàm CSV reader thông thường `spark.read.csv()`.
  - Dùng `unionByName(allowMissingColumns=True)` thô sơ mà không có Delta Lake Metadata quản lý schema. Dữ liệu cũ bị gán NULL một cách mất kiểm soát, không có schema enforcement.
* **Thời gian đọc:** 15.29 giây cho 1,020,000 dòng.

---

### 3.2. Vấn đề 2: Offline Duplicates (Bản ghi rác trùng lặp 2%)
* **Thực trạng dữ liệu:**
  - Trong quá trình mạng truyền tải hoặc hệ thống ingest retry, có **20,904 bản ghi trùng lặp** bị tiêm vào dữ liệu (chiếm **2.05%** tổng volume).
  - Bản ghi trùng lặp có cùng `user_id`, `event_time`, `product_id`, và `event_type`.
* **Cách Baseline xử lý:**
  - Hoàn toàn **KHÔNG LÀM SẠCH (NO DEDUPLICATION)**.
  - Cả 20,904 bản ghi rác được đẩy thẳng vào các phép tính toán downstream.
* **Hậu quả nghiệp vụ:** Doanh thu (`f_spend_30d`) và số lượng order (`f_purchases_30d`) của khách hàng bị đội khống lên 2%, làm sai lệch hoàn toàn nhãn huấn luyện ML.

---

### 3.3. Vấn đề 3: High Cardinality & Tràn bộ nhớ (Shuffle Spill)
* **Thực trạng dữ liệu:**
  - Cột `category_id` sâu tới 4 tầng phân cấp (ví dụ: `appliances.kitchen.refrigerators...`) với hàng chục ngàn giá trị rời rạc.
* **Cách Baseline xử lý:**
  - Thực hiện trực tiếp phép tính:
    ```sql
    COUNT(DISTINCT category_id) AS f_distinct_categories_30d
    ```
    mà không cắt tỉa về cấp 1 (`category_level1`) và không dùng thuật toán xấp xỉ HyperLogLog (`approx_count_distinct`).
* **Hậu quả trên Spark UI:**
  - Bảng băm (Hash Aggregation Table) trong bộ nhớ của Executor vượt quá ngưỡng 1GB.
  - Spark MemoryManager liên tục phát cảnh báo tràn RAM:
    ```
    WARN MemoryManager: Total allocation exceeds 95.00% (1,020,054,720 bytes) of heap memory
    Scaling row group sizes to 63.33% for 12 writers
    ```
  - Dữ liệu bị đẩy ngược xuống đĩa cứng (**Shuffle Spill to Disk**), làm tắc nghẽn I/O hệ thống.

---

### 3.4. Vấn đề 4: Data Skew & Hiện tượng Task Straggler
* **Thực trạng dữ liệu:**
  - Ngành hàng `electronics.smartphone` chiếm tới **40.27%** toàn bộ giao dịch của sàn thương mại điện tử.
* **Cách Baseline xử lý:**
  - Thực hiện Shuffle Join giữa bảng sự kiện (`df_raw`) và bảng danh mục sản phẩm (`dim_categories`) trên cột `category_code`.
  - Do đã tắt Broadcast Join và tắt AQE Skew Join, Spark băm dữ liệu theo hash key vào 200 partition.
  - Toàn bộ hơn 400,000 dòng của `electronics.smartphone` bị dồn vào **đúng 1 partition duy nhất**!
* **Hậu quả trên Spark UI (Task Straggler):**
  - Trong Stage 14, 199 tasks xử lý các partition nhỏ hoàn thành trong vòng vài chục mili-giây.
  - Đúng 1 task chứa partition bị Skew phải xử lý 40% khối lượng dữ liệu, chạy lẹt đẹt kéo dài toàn bộ thời gian của Stage lên **19.01 giây**!
  - Trên **Event Timeline** của Spark UI, task này kéo dài một vệt thẳng tắp (Straggler), trong khi các CPU core khác phải ngồi chơi chờ đợi.

---

## 4. KẾT QUẢ THỰC NGHIỆM ĐO LƯỜNG (BENCHMARK)

Bảng thông số thực tế đo được từ lần chạy `src/spark/spark_baseline.py`:

| Tiêu chí đo lường | Số liệu thực tế ghi nhận (Baseline) | Đánh giá & Vấn đề phát hiện |
| :--- | :---: | :--- |
| **Tổng thời gian chạy (Total Pipeline)** | **79.33 giây** | Quá chậm đối với quy mô 1 triệu dòng. |
| **Thời gian đọc CSV & Schema Check** | **15.29 giây** | Chậm do đọc file thô chưa chuyển đổi cột. |
| **Tỷ lệ Duplicate rác sót lại** | **2.05% (20,904 dòng)** | Chưa làm sạch, làm bẩn Feature Store. |
| **High Cardinality Aggregation** | **3.58 giây** | Tràn bộ nhớ Heap > 95%, Spill ra Disk. |
| **Skewed Shuffle Join (Stage 14)** | **19.01 giây** | Xuất hiện **Task Straggler** kéo dài Stage. |
| **Thời gian tính Gold `feat_user_30d`** | **25.50 giây** | Partition cố định 200 làm phân mảnh I/O. |
| **Shuffle Spill (Memory & Disk)** | **CÓ (Cảnh báo vàng/đỏ)** | Heap allocation vượt 95%, ép ghi đĩa tạm. |

---

## 5. HÌNH ẢNH MINH CHỨNG SPARK UI THỰC TẾ (NỘP RUBRIC)

Dưới đây là ảnh chụp thực tế từ hệ thống khi chạy `python src/spark/spark_baseline.py`:

### Minh chứng: Data Skew & Task Straggler trên Spark UI (Stage 14)
* **Vị trí:** Tab **Stages** -> **Stage 14** (`http://localhost:4040/stages/stage/?id=14&attempt=0`).
* **Bằng chứng rõ rệt:**
  - Cột **Shuffle Read Records**: `Median` chỉ có `0 records`, nhưng `Max` vọt lên tới **`284,217 records`** (Task 597 ôm trọn ngành điện thoại)!
  - Cột **Duration**: `Median` là `0.1s`, trong khi `Max` là **`2s` (2.37s)** — lệch gấp **20 LẦN**!
  - Trên **Event Timeline**: Task chạy đơn độc suốt 2.3 giây trong khi các CPU core khác rảnh rỗi chờ đợi.

![Minh chứng Spark UI Stage 14 Task Straggler do Data Skew](screenshots/16_spark_baseline_stage14_skew.png)

