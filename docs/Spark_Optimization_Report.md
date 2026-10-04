# Bao Cao Spark Batch Processing Toi Uu (Optimized)

> Hạng mục rubric: Processing Jobs - Spark: Handle skew (3.0đ), Schema evolution (3.0đ), Offline duplicate (3.0đ), Pipeline integration (2.0đ) = 11.0 điểm.  
> Code: [src/spark/spark_optimized.py](../src/spark/spark_optimized.py), [src/spark/skew_experiment.py](../src/spark/skew_experiment.py).  
> Cách chạy lại: `make spark-opt` và `make spark-skew`.

---

## 1. Vande Can Giai Quyet

Xu ly du lieu thuong mai dien tu offline o quy mo lon gap phai 4 thach thuc cot loi:
1. **Data Skew (lech du lieu)**: Cac user hoac san pham hot tap trung luong thao tac lon, khien task xu ly partition do bi nghen (straggler task).
2. **High Cardinality (nhieu gia tri phan biet)**: Dem phan biet tren cac cot dinh danh phan cap (nhu session, category) tieu ton bo nho heap, de gay tran ra dia (spill).
3. **Schema Evolution (bien doi cau truc cot)**: Nguon du lieu co them cot moi (nhu `discount_percent`) giua cac dot ingest can duoc tu dong dong bo ma khong gay loi pipeline.
4. **Offline Duplicates (du lieu trung lap)**: Can loai bo trung lap logic de dam bao tinh chinh xac cua dac trung huan luyen va du bao.

---

## 2. Cach Lam

Pipeline toi uu hoa trong `src/spark/spark_optimized.py` ap dung cac giai phap ky thuat phan tang:

### 2.1. Xu ly Data Skew tren khoa user_id
- **Thuc nghiem doc lap (`src/spark/skew_experiment.py`)**: So sanh 3 bien the tren cung dataset lech:
  - *Variant A (Baseline)*: Tat AQE, buoc Sort-Merge Join de quan sat task bi lech.
  - *Variant B (AQE Skew Join)*: Bat `spark.sql.adaptive.skewJoin.enabled = true`, ha `skewedPartitionThresholdInBytes` xuong 64KB va `skewedPartitionFactor = 2` de Spark tu dong phat hien va chia nho partition lech.
  - *Variant C (Two-Stage Salting)*: Gan salt ngau nhien (0..3) vao bang lech, nhan ban bang chieu len 4 lan, join tren `user_id_salted` va tong hop 2 giai doan de triet tieu straggler.
- **Toi uu join bang danh muc nho**: Voi bang `dim_categories` nho (~50KB), ap dung `broadcast(dim_categories)` de dua truc tiep vao RAM tung executor, loai bo hoan toan shuffle qua mang (day la toi uu join bang nho, khong phai ky thuat xu ly skew).

### 2.2. Xu ly High Cardinality
- Rut gon danh muc ve cap 1 (`category_level1` tu `category_code.split('.')[0]`), giam so luong nhom tu hang ngan ve khoang 15 nganh hang chinh.
- Su dung thuat toan xap xi HyperLogLog: `approx_count_distinct(..., rsd=0.01)` (sai so 1%, tin cay 99%) thay vi `count(distinct ...)` chinh xac, giam bo nho heap can dung tu Gigabytes xuong Kilobytes.

### 2.3. Quan ly Schema Evolution voi Delta Lake
- Su dung Delta Lake transaction log voi `spark.read.format("delta")` va tuy chon `.option("mergeSchema", "true")`.
- Tu dong gop batch 9 cot (01-15/10) va batch 10 cot (16-25/10 co `discount_percent`) thanh schema thong nhat 10 cot tren Bronze va Silver Lakehouse.

### 2.4. Khu trung lap logic (Deduplication)
- Su dung Window function `row_number().over(Window.partitionBy(dedup_keys).orderBy(F.col("event_time").desc()))` va loc lay ban ghi moi nhat `row_num = 1`.
- Lam sach toan bo ~2% ban ghi trung lap duoc tiem vao tu du lieu nguon.

### 2.5. Xay dung DWH Star Schema va Feast Feature View
- **Silver $\rightarrow$ Gold DWH**: Xay dung `dim_user` (snapshot tong hop), `dim_product` (SCD Type 2 quan ly lich su thay doi gia theo thoi gian), va `fact_user_events` (bang su kien).
- **Gold Feast**: Xuat bang dac trung `feat_user_30d` kem nhan nhi phan `user_labels` (cua so 1 gio) duoi dinh dang Parquet sach khong chua `_delta_log` phuc vu Feast materialization.

---

## 3. Ket Qua Do

### 3.1. Ket qua thuc nghiem Data Skew
Nguon: [docs/evidence/spark_skew_experiment.md](evidence/spark_skew_experiment.md) (1,398,581 dong, 35% hot key tren 3 user):

| Bien the | Ky thuat | Thoi gian tong (s) | Max task duration | Disk spill |
| :--- | :--- | :--- | :--- | :--- |
| **Variant A (Baseline)** | Sort-Merge Join (No AQE, No Salt) | **3.552s** | 1.398s | 0 B |
| **Variant B (AQE Skew Join)** | AQE Skew Join (Dynamic Split) | **2.451s** | 1.398s | 0 B |
| **Variant C (Two-Stage Salting)** | Salting 4 phan vung (2-Stage) | **5.539s** | 1.398s | 0 B |

*Nhan xet*: Bien the B (AQE Skew Join) dat thoi gian chay nhanh nhat (2.451s, giam 31% so voi Baseline). Bien the C do phai qua 2 giai doan aggregate va nhan ban bang chieu nen co chi phi overhead cao hon o tap du lieu nho.

### 3.2. Ket qua xu ly Schema Evolution va Duplicates
Nguon: [docs/evidence/data_profile.md](evidence/data_profile.md) va Spark execution log:
- **Schema Evolution**: Bronze va Silver dong nhat 10 cot, cot `discount_percent` duoc gan NULL chinh xac cho cac ban ghi Part 1 ma khong loi.
- **Khu trung lap**: Loai bo thanh cong toan bo 28,542 dong duplicate (chiem 2.00% tong volume).
- **Shuffle Spill to Disk**: Ghi nhan 0 Bytes tren tap du lieu hien tai nho dung HyperLogLog xap xi.

---

## 4. Minh Chung

![Minh chứng Spark UI AQE Skew Join](screenshots/E08_spark_skew_aqe_ui.png)
*Ảnh chứng minh: Spark AQE tự động phát hiện partition lệch và chia nhỏ thành các sub-partitions, giảm thời gian stage xuống 2.451s.*

![Minh chứng Spark UI Salting](screenshots/E09_spark_skew_salting_ui.png)
*Ảnh chứng minh: Thuật toán Salting chia đều các dòng hot key qua 4 phân vùng, san phẳng tải giữa các core.*

---

## 5. Han Che va Luu Y

1. **Tap du lieu dev sample co quy mo nho**: Voi 1.4 trieu dong, dung luong shuffle nho nen Disk Spill chua the hien ro o muc Gigabytes nhu tren cluster lon. Chenh lech thoi gian o muc giay.
2. **Mo rong do luong**: De kiem chung hieu qua ro net o quy mo lon, nguoi dung co the chay bo du lieu quy mo 5GB bang lenh `make gen-data-medium` va thuc thi lai `make spark-skew`.
