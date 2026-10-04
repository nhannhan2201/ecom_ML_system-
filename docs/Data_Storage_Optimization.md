# Bao Cao Toi Uu Hoa Luu Tru (Data Storage Optimization)

> Hạng mục rubric: Data Storage: Lakehouse compaction & Z-order (3.0đ), Datawarehouse indexing (3.0đ) = 6.0 điểm.  
> Code: [src/spark/spark_optimized.py](../src/spark/spark_optimized.py), [scripts/optimize_storage.py](../scripts/optimize_storage.py), [scripts/dwh_explain_analyze.py](../scripts/dwh_explain_analyze.py).  
> Cách chạy lại: `python3 scripts/optimize_storage.py` và `python3 scripts/dwh_explain_analyze.py`.

---

## 1. Vande Can Giai Quyet

He thong luu tru du lieu thuong mai dien tu o ca hai tang lakehouse va data warehouse thuong gap hai van de:
1. **Van de tap tin nho tren Lakehouse (Small Files Problem)**: Cac batch ghi lien tuc tao ra nhieu file Parquet nho gay qua tai metadata RPC tren storage va giam hieu qua doc.
2. **Quet toan bo bang tren Data Warehouse (Sequential Full Table Scan)**: Khi bang Fact tang len hang trieu ban ghi, cac truy van lich su hanh vi khach hang neu khong co index se phai doc toan bo dia, gay nghen buffer I/O.

---

## 2. Cach Lam

Chi tiet hai tang toi uu hoa:

### 2.1. Tang Data Lakehouse (Delta Lake / MinIO)
- **Phan vung (Partitioning)**: Bang `fact_user_events` duoc phan vung theo ngay (`partitionBy("date")`), giup Spark bo qua cac phan vung khong can thiet khi truy van theo thoi gian (partition pruning).
- **Gom file nho (Compaction)**: Thuc hien `OPTIMIZE` de gom cac file nho thanh file co kich thuoc tieu chuan (~40-45 MB/file).
- **Sap xep da chieu (Z-Ordering)**: Thuc thi `OPTIMIZE delta.fact_user_events ZORDER BY (user_id)` theo thuat toan Hilbert Curve, giup Delta Lake luu tru min/max cua `user_id` tren tung file de bo qua cac file khong chua user khi tim kiem (Data Skipping).
- **VACUUM an toan**: Mac dinh trong pipeline DP2/DP3 su dung `VACUUM ... RETAIN 168 HOURS` (bien `DELTA_VACUUM_RETAIN_HOURS=168`) va giu nguyen kiem tra thoi gian luu tru de bao ve Delta Time Travel. Chi su dung `RETAIN 0 HOURS` khi can demo don dep ngay lap tuc qua co `--demo-vacuum` trong `scripts/optimize_storage.py`.

### 2.2. Tang Data Warehouse (PostgreSQL 15)
- **Composite B-Tree Index**: Tao chi muc ket hop tren bang `gold.fact_user_events`:
  ```sql
  CREATE INDEX idx_fact_user_events_user_time 
  ON gold.fact_user_events (user_id, event_time DESC);
  ```
- **Cap nhat thong ke CBO**: Chay `ANALYZE gold.fact_user_events;` de Postgres Cost-Based Optimizer co thong tin phan bo du lieu chinh xac.

---

## 3. Ket Qua Do

### 3.1. Ket qua toi uu Lakehouse
Nguon: [docs/evidence/lakehouse_inspection.txt](evidence/lakehouse_inspection.txt) (Bang `fact_user_events`, User ID: 513359812):

| Tieu chi danh gia | Truoc toi uu (Before) | Sau toi uu (After) | Ghi chu |
| :--- | :--- | :--- | :--- |
| **So file Parquet hoat dong** | 3 files (phan manh) | 3 files (da gom) | Nhanh hon khi so partition goc tang |
| **Kich thuoc trung binh/file** | 42.05 MB/file | 42.05 MB/file | Kich thuoc tieu chuan cho I/O |
| **Sap xep da chieu** | Chua sap xep | Z-Order (`user_id`) | Thu tu Hilbert Curve |
| **Data Skipping** | Khong kich hoat | Kich hoat | Bo qua cac file ngoai dai user_id |
| **Thoi gian truy van user** | 3.011s | 1.575s | **Nhanh hon 1.9 lan** |
| **Che do VACUUM** | - | RETAIN 168h | Giu an toan Time Travel 7 ngay |

### 3.2. Ket qua toi uu Data Warehouse (PostgreSQL EXPLAIN ANALYZE)
Nguon: [docs/evidence/dwh_explain_analyze.txt](evidence/dwh_explain_analyze.txt) (Truy van lay 355 su kien cua user 513359812 tren 1.4 trieu dong):

| Tieu chi | Baseline (Parallel Seq Scan) | Optimized (Bitmap Index Scan) | Cai thien |
| :--- | :--- | :--- | :--- |
| **Kieu thuc thi** | Parallel Seq Scan (2 workers) | Bitmap Index Scan (`idx_fact_user_events_user_time`) | Dung B-Tree Index |
| **Shared Buffers Read** | 35,878 blocks | 27 blocks (hit 3, read 27) | **Giam hon 1,300 lan I/O** |
| **Rows Removed by Filter**| 466,075 rows | 0 rows | Khong quet dong thua |
| **Thoi gian lap ke hoach** | 2.446 ms | 0.177 ms | Nhanh hon |
| **Thoi gian thuc thi** | **395.701 ms** | **2.659 ms** | **Nhanh hon ~148 lan** |

---

## 4. Minh Chung

Minh chung duoc ghi nhan tu ket qua terminal va cong cu quan tri:

![Minh chứng Lakehouse Storage Optimize](screenshots/E10_spark_lakehouse_storage.png)
*Ảnh chứng minh: Thực thi lệnh OPTIMIZE ZORDER BY user_id trên MinIO Delta Lake gom gọn các file parquet.*

![Minh chứng Lakehouse Inspection](screenshots/E14_lakehouse_inspection.png)
*Ảnh chứng minh: Bảng tổng hợp số bản ghi và phân vùng Lakehouse qua script inspect_lakehouse.py.*

![Minh chứng DWH EXPLAIN ANALYZE](screenshots/E15_dwh_explain_analyze.png)
*Ảnh chứng minh: Kế hoạch thực thi EXPLAIN (ANALYZE, BUFFERS) chuyển từ Parallel Seq Scan (395ms) sang Bitmap Index Scan (2.6ms).*

---

## 5. Han Che va Luu Y

1. **Z-Order tren tap du lieu nho**: Tren bo du lieu dev sample, so file dang hoat dong it (3 files) nen hieu qua data skipping the hien o muc bo qua 1-2 file. O quy mo du lieu lon hon (hang chuc GB voi hang tram file), ty le file bi bo qua se vuot troi hon.
2. **Chi phi ghi cua Index**: Chi muc composite B-Tree tren DWH giup tang toc doc 148 lan nhung se lam tang nhe thoi gian ghi (INSERT batch). Do do can chay batch insert vao DWH truoc roi moi tao index hoac cap nhat dinh ky.
