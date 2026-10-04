# Báo Cáo Đo Lường Dữ Liệu Thực Tế (Data Profiling Evidence)

> **Thời điểm đo lường**: `2026-10-04T02:28:46.182100Z`  
> **Công cụ đo**: `scripts/profile_generated_data.py` (Đo lường trực tiếp trên dữ liệu thật sinh ra từ REES46).

---

## 1. Quy Mô & Dung Lượng (Volume Metric)

| Phân Đoạn Dữ Liệu | Số Dòng (Rows) | Dung Lượng (Bytes) | Dung Lượng (MB) | Số Cột |
| :--- | :--- | :--- | :--- | :--- |
| **Part 1 (01/10 - 15/10)** | 5,100 | 682,716 B | 0.65 MB | 9 cột |
| **Part 2 (16/10 - 25/10)** | 5,100 | 693,096 B | 0.66 MB | 10 cột |
| **TỔNG CỘNG** | **10,200** | **1,375,812 B** | **1.31 MB** | - |

---

## 2. Minh Chứng Schema Evolution (2đ Rubric)

- **Schema Part 1 (9 cột nguyên bản)**: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session`
- **Schema Part 2 (10 cột tiến hóa)**: `event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session, discount_percent`
- **Cột mới xuất hiện tại Part 2**: `discount_percent` (Bắt đầu từ ngày 16/10, trước đó không tồn tại).

---

## 3. Minh Chứng Tiêm Lỗi Duplicate (2đ Rubric)

| Phân Đoạn | Số Dòng Duplicate Đầy Đủ | Tỷ Lệ Thực Tế (%) | Mục Tiêu Cấu Hình |
| :--- | :--- | :--- | :--- |
| **Part 1** | 103 dòng | **2.02%** | ~2.00% |
| **Part 2** | 100 dòng | **1.96%** | ~2.00% |
| **Trùng lặp theo Khóa Logic** | 204 dòng | **2.00%** | ~2.00% |

---

## 4. Phân Tích Độ Lệch Khóa (Skewness Analysis)

### A. Phân phối hành vi người dùng (`event_type` - Class Imbalance):
- **`view`**: 9,863 lượt (96.70%)
- **`cart`**: 188 lượt (1.84%)
- **`purchase`**: 149 lượt (1.46%)

### B. Top ngành hàng (`category_code`):
- **`nan`**: 3,261 lượt (31.97%)
- **`electronics.smartphone`**: 3,003 lượt (29.44%)
- **`electronics.audio.headphone`**: 328 lượt (3.22%)
- **`electronics.clocks`**: 327 lượt (3.21%)
- **`computers.notebook`**: 306 lượt (3.00%)

### C. Top thương hiệu (`brand`):
- **`samsung`**: 1,392 lượt (13.65%)
- **`nan`**: 1,297 lượt (12.72%)
- **`apple`**: 1,064 lượt (10.43%)
- **`xiaomi`**: 765 lượt (7.50%)
- **`huawei`**: 282 lượt (2.76%)

---

## 5. Phân Tích Lực Lượng Cao (High-Cardinality Analysis)

| Cột (Column) | Số Giá Trị Không Rỗng | Số Giá Trị Duy Nhất (Unique) | Tỷ Lệ Cardinality (Unique/Total) | Phân Loại |
| :--- | :--- | :--- | :--- | :--- |
| `event_time` | 10,200 | 1,966 | 0.192745 | High Cardinality |
| `event_type` | 10,200 | 3 | 0.000294 | Low/Medium Cardinality |
| `product_id` | 10,200 | 4,260 | 0.417647 | High Cardinality |
| `category_id` | 10,200 | 393 | 0.038529 | Low/Medium Cardinality |
| `category_code` | 6,939 | 106 | 0.015276 | Low/Medium Cardinality |
| `brand` | 8,903 | 682 | 0.076603 | Low/Medium Cardinality |
| `price` | 10,200 | 2,909 | 0.285196 | High Cardinality |
| `user_id` | 10,200 | 3,376 | 0.330980 | High Cardinality |
| `user_session` | 10,200 | 3,642 | 0.357059 | High Cardinality |
| `discount_percent` | 5,100 | 5 | 0.000980 | Low/Medium Cardinality |

---

## 6. Đặc Tính Streaming Generator (Online Feeder Specs)

| Hạng Mục Tiêm Lỗi Stream | Cấu Hình Kỹ Thuật | Ý Nghĩa Nghiệp Vụ / Mô Phỏng |
| :--- | :--- | :--- |
| **Burst Traffic** | x30 (kéo dài 600s) | Mô phỏng Flash Sale đột biến thông lượng |
| **Late Arrival** | 5.0% sự kiện, trễ 5 - 10 phút | Mô phỏng trễ gói tin di động qua event-time buffer |
| **Streaming Duplicate** | 1.5% duplicate | Mô phỏng network retry từ mobile client |
| **Kafka Routing** | Partition Key: `user_id` | Đảm bảo đúng thứ tự sự kiện trên cùng một user |
