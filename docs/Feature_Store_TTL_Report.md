# Bao Cao Kien Truc Feast Feature Store va Thiet Ke TTL

> Hang muc rubric: Feature Store Architecture & Push Pipeline (Incremental Materialize 2.0d, Push to Offline 1.0d, Push to Online 1.0d, TTL Justification 2.0d = 6.0 diem).  
> Code: [feature_store/features.py](../feature_store/features.py), [feature_store/materialize.py](../feature_store/materialize.py), [scripts/feast_serving_benchmark.py](../scripts/feast_serving_benchmark.py).  
> Cach chay lai: `python3 feature_store/materialize.py` va `python3 scripts/feast_serving_benchmark.py`.

---

## 1. Vande Can Giai Quyet

Trong he thong du doan xac suat mua hang thoi gian thuc, mo hinh can ket hop ca dac trung lich su dai han (30 ngay) va dac trung tuong tac tuc thoi (15 phut). Neu khong co Feature Store:
1. **Lech du lieu giua train va serve (Training-Serving Skew)**: Code tinh toan dac trung luc huan luyen va luc suy luan bi lech nhau.
2. **Ro ri du lieu qua khu (Data Leakage)**: Khi ghep dac trung vao nhan qua khu khong tuan thu point-in-time correctness.
3. **Thoi gian song (TTL) khong phu hop**: Neu giu vo han se lam tran bo nho Redis; neu xoa qua som se mat dac trung khach hang khi ho quay lai website.

---

## 2. Cach Lam

### 2.1. Kien truc nguon du lieu Feast khong co duong dan cung
- **Tach biet Delta Lake va Feast FileSource**: Delta Lake luu lich su transaction tai `_delta_log/` va chua nhieu phien ban file cu (tombstones). Feast 0.38 FileSource khi doc truc tiep thu muc Delta co the doc phai cac file cu. Do do, Spark DP3 xuat them mot ban Parquet sach tai `s3://ecommerce-lakehouse/feast/user_batch_features_30d/` khong chua `_delta_log`.
- **Tham so hoa ngay tinh toan**: Khong de ngay cung trong code; `as_of_date` duoc truyen qua Airflow Variable `feature_as_of_date` (mac dinh tu dong lay ngay lon nhat trong Silver Lakehouse).

### 2.2. Thiet ke TTL (Time-To-Live) cho hai Feature Views
1. **`user_batch_features_30d` (TTL = 30 ngay)**:
   - *Ly do*: Chu ky mua sam cua nguoi dung thuong mai dien tu thuong lap lai theo thang (nhan luong dau/giua thang, cac dip Mega Sale). Khoang 30 ngay la du de ghi nhan thoi quen tieu dung ma khong bi drift phan bo du lieu.
   - *Bao ve Serving*: Neu Airflow gap su co ngung 1-2 ngay, dac trung tren Redis van con hieu luc phuc vu suy luan.
2. **`user_stream_features_15m` (TTL = 2 gio)**:
   - *Ly do*: Hanh vi xem va them gio hang trong 15 phut the hien y dinh mua hang ngay trong phien (impulse buying). Thoi gian song 2 gio du de bao quat toan bo session hien tai cua khach hang, sau do tu dong giai phong RAM tren Redis de tieu thu tai nguyen toi uu.

### 2.3. Co che Materialize gia tang (Incremental)
- Feast tu dong luu tru thoi diem da nap gan nhat vao `registry.db`. Khi chay `store.materialize_incremental(end_date)`, Feast chi quet va nap cac ban ghi moi phat sinh tu checkpoint cu den `end_date`, khong quet lai toan bo tap du lieu lich su.

---

## 3. Ket Qua Do

### 3.1. Do luong Feast Incremental Materialization
Nguon: [docs/evidence/feast_incremental.txt](evidence/feast_incremental.txt) (173,941 dac trung nguon tu MinIO Lakehouse):

| Lan chay | Che do | Moc thoi gian nap | So keys truoc | So keys sau | Delta nap moi | Thoi gian chay |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Run 1 (Backfill D)** | Range | 2019-10-01 -> 2019-10-25 23:59:59 | 0 keys | 173,941 keys | **+173,941** | 15.82s |
| **Run 2 (Incremental D+1)** | Incremental | 2019-10-25 23:59:59 -> 2019-10-26 23:59:59 | 173,941 keys | 173,941 keys | **+0 (khong trung)**| **3.88s (4x faster)** |

*Nhan xet*: Lan 2 chi kiem tra khoang thoi gian delta tu checkpoint cu, thoi gian thuc thi giam 4 lan va khong nap trung lap 173,941 keys da co tren Redis.

### 3.2. Do luong do tre Online Serving tu Redis
Nguon: [docs/evidence/feast_incremental.txt](evidence/feast_incremental.txt) qua `scripts/feast_serving_benchmark.py`:
- **So dac trung truy van**: 9 dac trung (5 batch 30d + 4 stream 15m).
- **Do tre trung binh (Average Latency)**: **1.02 ms** (qua 10 lan goi).
- **Do tre tot nhat (Best Latency)**: **0.79 ms**.
- **Danh gia SLA (< 5 ms)**: Dat yeu cau phuc vu mo hinh online.

---

## 4. Minh Chung

Minh chung chup tu giao dien va log thuc thi Feast:

![Minh chứng Feast Incremental Materialization](screenshots/E29_feast_incremental_proof.png)
*Ảnh chứng minh: Log thực thi materialize đồng bộ đặc trưng từ MinIO Parquet lên Redis Online Store theo cơ chế incremental checkpoint.*

![Minh chứng Online Serving Benchmark](screenshots/E30_feast_serving_latency.png)
*Ảnh chứng minh: Truy vấn 9 đặc trưng trực tiếp từ Redis Online Store đạt độ trễ trung bình 1.02ms (< 2ms).*

---

## 5. Han Che va Luu Y

1. **Che do Localhost vs Docker**: FileSource can cau hinh `s3_endpoint_override` dong de chay thong suot ca tu moi truong host (`localhost:9000`) lan ben trong mang Docker (`minio:9000`). Script `materialize.py` da duoc bo sung co che tu dong phat hien nay.
2. **Kiem soat dung luong RAM Redis**: Voi 173,941 user keys tren Redis, dung luong chiem dung khoang 35 MB RAM. Voi tap du lieu full (> 10 trieu users), can ap dung maxmemory-policy `volatile-lru` hoac `allkeys-lru` tren Redis de tu dong giai phong bo nho theo TTL.
