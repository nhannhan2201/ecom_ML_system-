# Bao Cao Spark Batch Processing Baseline (Chua Toi Uu)

> Hang muc rubric: Spark job to handle offline data problems (Baseline without optimization, 2.0 diem).  
> Code: [src/spark/spark_baseline.py](../src/spark/spark_baseline.py), [src/spark/skew_experiment.py](../src/spark/skew_experiment.py).  
> Cach chay lai: `make spark-skew`.

---

## 1. Vande Can Giai Quyet

Khi xu ly du lieu thuong mai dien tu quy mo lon bang Apache Spark ma giu nguyen cau hinh mac dinh hoac thiet ke ngau nhien (naive), he thong se gap 4 tro ngai lon:
1. **Data Skew (lech du lieu)**: Mot vai user hoac nganh hang co so luong su kien vuot troi lam mot so task bi qua tai (straggler task) keo dai toan bo stage.
2. **High Cardinality (so luong gia tri phan biet lon)**: Dem phan biet tren cac cot phan cap sau lam ton bo nho heap, dan toi nguy co shuffle spill xuong dia.
3. **Schema Evolution (thay doi cau truc cot)**: Nguon du lieu them bot cot theo thoi gian ma khong co metadata lakehouse quan ly lam loi doc file hoac mat mat thong tin.
4. **Offline Duplicates (du lieu trung lap)**: Cac su kien bi ghi trung do loi mang hoac retry gay sai lech nhan du doan downstream.

---

## 2. Cach Lam (Anti-Patterns trong Baseline)

Trong ban Baseline, pipeline mo phong cach tiep can chua duoc toi uu:

1. **Cau hinh tat cac co che thich ung**:
   - `spark.sql.adaptive.enabled = false` (tat AQE).
   - `spark.sql.adaptive.skewJoin.enabled = false` (khong tu dong chia nho partition lech).
   - `spark.sql.autoBroadcastJoinThreshold = -1` (ep buoc tat ca phep join phai dung Shuffle Sort-Merge Join, ke ca bang danh muc nho).
   - So luong shuffle partition co dinh o muc 8 hoac 200 partition ma khong tu dong co gian.

2. **Khong xu ly duplicate va khong quan ly schema**:
   - Doc CSV tho bang `spark.read.csv` va `unionByName` khong co kiem soat kieu du lieu.
   - Bo qua buoc loc trung lap logic, de nguyen ban ghi trung di vao bang tinh tinh toan dac trung.

3. **Dem chinh xac tren cot co cardinality lon**:
   - Dung `COUNT(DISTINCT category_id)` truc tiep thay vi cat tia level hoac dung xap xi.

---

## 3. Ket Qua Do

### 3.1. Do luong thuc nghiem Data Skew (Sort-Merge Join tren khoa user_id)
Nguon so lieu: [docs/evidence/spark_skew_experiment.md](evidence/spark_skew_experiment.md) (1,398,581 dong, 35% hot key tren 3 user).

| Bien the | Ky thuat | Thoi gian tong (s) | Max task duration | Disk spill |
| :--- | :--- | :--- | :--- | :--- |
| **Variant A (Baseline)** | Sort-Merge Join (AQE OFF, No Salting) | **3.552s** | 1.398s | 0 B |
| **Variant B (AQE Skew Join)** | AQE Skew Join (Dynamic splitting) | **2.451s** | 1.398s | 0 B |
| **Variant C (Two-Stage Salting)** | Salting 4 phan vung | **5.539s** | 1.398s | 0 B |

*Ghi chu: Cac chi so ve spill o quy mo du lieu lon (medium 5GB) se duoc bo sung sau khi chay `make gen-data-medium`.*

### 3.2. Tong quan pipeline baseline tong the
Nguon: Do luong pipeline baseline cu the tren may cuc bo:
- Thoi gian doc CSV tho: TBD (chay `python3 src/spark/spark_baseline.py` de do lai).
- Ti le duplicate sot lai: 2.05% tren tap du lieu sinh ra neu khong loc (xem [docs/evidence/data_profile.md](evidence/data_profile.md)).

---

## 4. Minh Chung

Minh chung duoc ghi nhan tu giao dien Spark UI (`http://localhost:4040`) khi thuc thi bien the Baseline:

![Minh chứng Spark UI Stage Skew Baseline](screenshots/E07_spark_baseline_ui.png)
*Ảnh chứng minh: Stage thực hiện Sort-Merge Join trên dữ liệu lệch khi tắt AQE. Trên Event Timeline xuất hiện task straggler kéo dài do phải nhận phần lớn bản ghi của hot key.*

---

## 5. Han Che va Luu Y

1. **Quy mo du lieu cuc bo nho**: Tren dataset `small` (~1.4 trieu dong), tong dung luong shuffle chi vai chuc MB, chua gay tran bo nho thuc te (spill to disk = 0 B). Chenh lech thoi gian tuyet doi giua cac bien the nam o muc vai giay.
2. **Kiem chung o quy mo lon**: De quan sat ro spill xuong dia va straggler keo dai hang phut, can chay bo du lieu quy mo lon bang lenh `make gen-data-medium` (5 GB) roi chay lai `make spark-skew`.
