# Bao Cao Do Luong Du Lieu Thuc Te (Data Profile Evidence)

> **Thoi diem do luong**: `2026-10-04 03:20:20 UTC`  
> **Lenh da chay**: `make profile-data` (`scripts/profile_generated_data.py`)  
> **Git commit**: `c39009f`  
> **Che do du lieu (Mode)**: `small`  
> **Tong so dong nguon**: `1,020,000` dong  
> **Manifest timestamp**: `2026-10-04T03:20:09.888930Z`  
> **Nguon du lieu goc**: REES46 eCommerce Behavior Data (2019-Oct.csv)  
> - Part 1: 2019-10-01 den 2019-10-15 (Schema 9 cot)  
> - Part 2: 2019-10-16 den 2019-10-25 (Schema 10 cot, co discount_percent)  

---

## 1. Quy Mo va Dung Luong (Volume Metric)

| Phan Doan | So Dong (Rows) | Dung Luong (Bytes) | Dung Luong (MB) | So Cot |
| :--- | :--- | :--- | :--- | :--- |
| **Part 1 (01/10 - 15/10)** | 510,000 | 68,131,903 B | 64.98 MB | 9 cot |
| **Part 2 (16/10 - 25/10)** | 510,000 | 69,571,355 B | 66.35 MB | 10 cot |
| **TONG CONG** | **1,020,000** | **137,703,258 B** | **131.32 MB** | - |

---

## 2. Minh Chung Schema Evolution (2d Rubric)

- **Schema Part 1 (9 cot nguyen ban)**: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`
- **Schema Part 2 (10 cot tien hoa)**: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session, discount_percent`
- **Cot moi xuat hien tai Part 2**: `discount_percent` (Bat dau tu ngay 16/10).

---

## 3. Minh Chung Tiem Loi Duplicate (2d Rubric)

| Phan Doan | So Dong Duplicate Toan Hang | Ty Le Thuc Te (%) | Muc Tieu Cau Hinh |
| :--- | :--- | :--- | :--- |
| **Part 1** | 10,288 dong | **2.02%** | ~2.00% |
| **Part 2** | 10,171 dong | **1.99%** | ~2.00% |
| **Trung lap theo Khoa Logic** | 20,904 dong | **2.05%** | ~2.00% |

---

## 4. Phan Tich Do Lech Khoa (Skewness Analysis)

### A. Phan phoi hanh vi nguoi dung (event_type - Class Imbalance):
- **`view`**: 971,258 luot (95.22%)
- **`cart`**: 26,301 luot (2.58%)
- **`purchase`**: 22,441 luot (2.20%)

### B. Top nganh hang (category_code):
- **`nan`**: 315,348 luot (30.92%)
- **`electronics.smartphone`**: 284,097 luot (27.85%)
- **`electronics.clocks`**: 31,569 luot (3.09%)
- **`computers.notebook`**: 28,347 luot (2.78%)
- **`electronics.audio.headphone`**: 28,161 luot (2.76%)

### C. Top thuong hieu (brand):
- **`nan`**: 138,857 luot (13.61%)
- **`samsung`**: 133,127 luot (13.05%)
- **`apple`**: 106,733 luot (10.46%)
- **`xiaomi`**: 73,087 luot (7.17%)
- **`huawei`**: 27,307 luot (2.68%)

### D. Top nguoi dung hoat dong (user_id):
- **`513359812`**: 361 luot (0.04%)
- **`522799138`**: 255 luot (0.03%)
- **`516415886`**: 254 luot (0.02%)
- **`543679273`**: 225 luot (0.02%)
- **`548750622`**: 224 luot (0.02%)

---

## 5. Phan Tich Luc Luong Cao (High-Cardinality Analysis)

| Cot (Column) | So Gia Tri Khong Rong | So Gia Tri Duy Nhat (Unique) | Ty Le Cardinality (Unique/Total) | Phan Loai |
| :--- | :--- | :--- | :--- | :--- |
| `event_time` | 1,020,000 | 54,336 | 0.053271 | Low/Medium Cardinality |
| `event_type` | 1,020,000 | 3 | 0.000003 | Low/Medium Cardinality |
| `product_id` | 1,020,000 | 67,511 | 0.066187 | Low/Medium Cardinality |
| `category_id` | 1,020,000 | 571 | 0.000560 | Low/Medium Cardinality |
| `category_code` | 704,652 | 124 | 0.000176 | Low/Medium Cardinality |
| `brand` | 881,143 | 2,374 | 0.002694 | Low/Medium Cardinality |
| `price` | 1,020,000 | 20,543 | 0.020140 | Low/Medium Cardinality |
| `user_id` | 1,020,000 | 173,941 | 0.170530 | High Cardinality |
| `user_session` | 1,020,000 | 234,619 | 0.230019 | High Cardinality |
| `discount_percent` | 510,000 | 5 | 0.000010 | Low/Medium Cardinality |
