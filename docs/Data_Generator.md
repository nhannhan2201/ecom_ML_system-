# Báo Cáo Module Sinh Dữ Liệu (Data Generator)
> Hạng mục rubric: Implement Data Generator (12.0 điểm). Mã nguồn: [batch_generator.py](file:///home/nhan/Projects/ecom_ML_system/src/generator/batch_generator.py), [stream_generator.py](file:///home/nhan/Projects/ecom_ML_system/src/generator/stream_generator.py). Cách chạy lại: `make gen-data` hoặc `make gen-data-skewed`.

## 1. Vấn đề cần giải quyết
Hệ thống xử lý dữ liệu lớn cần dữ liệu đầu vào mô phỏng chính xác các vấn đề thường gặp trong thực tế. Các vấn đề gồm dữ liệu lệch khóa (data skew), số lượng người dùng lớn (high cardinality), lược đồ thay đổi theo thời gian (schema evolution), bản ghi bị lặp do thiết bị gửi lại (duplicates), cùng các sự cố truyền luồng như dữ liệu đến muộn (late arrival) và lưu lượng tăng đột biến (traffic burst). Bộ sinh dữ liệu phải tạo ra tập dữ liệu kiểm thử đáp ứng đầy đủ các đặc tính này mà không làm tràn bộ nhớ.

## 2. Cách làm
Bộ sinh dữ liệu chia làm hai luồng riêng biệt: batch (theo lô) và streaming (theo luồng).

### 2.1 Sinh dữ liệu theo lô (Batch Generator)
- **Nguồn dữ liệu**: Sử dụng tập dữ liệu hành vi thương mại điện tử REES46 tháng 10/2019 (`2019-Oct.csv`).
- **Thay đổi lược đồ (Schema Evolution)**: Tách dữ liệu làm hai phần: Part 1 (từ 01/10 đến 15/10) gồm 9 cột gốc; Part 2 (từ 16/10 đến 25/10) bổ sung cột `discount_percent` (10 cột) để mô phỏng chiến dịch khuyến mãi.
- **Tăng cardinality và nhân bản**: Với mỗi replica $r > 0$, hàm băm xác định gán `user_id' = user_id + r * id_offset` cho 30% người dùng mới, 70% còn lại giữ nguyên để tạo người dùng quay lại. Giá sản phẩm dao động xác định trong khoảng $\pm 5\%$ để phục vụ kiểm thử SCD Type 2.
- **Tiêm trùng lặp (Duplicate)**: Tiêm ngẫu nhiên 2% số dòng lặp lại độc lập giữa các chunk bằng bộ sinh số ngẫu nhiên `SeedSequence([base_seed, replica, chunk, part])`.
- **Dữ liệu lệch khóa (Data Skew)**: Chế độ `--skewed` tập trung 30% lưu lượng vào 3 khóa nóng (`user_id`), đồng thời gán session cố định để kiểm thử join và shuffle.
- **Lưu trữ**: Dữ liệu được tải trực tiếp dạng luồng lên MinIO bucket `ecommerce-raw/batch/` qua S3 API mà không ghi trung gian ra đĩa.

### 2.2 Sinh dữ liệu thời gian thực (Streaming Generator)
- **Replay sự kiện**: Đọc dữ liệu từ ngày 26/10 đến 31/10/2019 và đẩy vào Kafka topic `ecommerce_stream_events` (3 partitions).
- **Dữ liệu đến muộn (Late Arrival)**: Sử dụng lớp `LateEventBuffer` giữ lại 5% sự kiện và gửi trễ từ 5 đến 10 phút so với thời gian phát sinh sự kiện.
- **Trùng lặp luồng (Streaming Duplicate)**: Tiêm 1.5% sự kiện bị gửi lại 2 lần để mô phỏng cơ chế at-least-once của mạng.
- **Lưu lượng đột biến (Burst Traffic)**: Tăng tốc độ phát x10 lần trong 30 giây để kiểm thử khả năng chịu tải và backpressure của Flink.

## 3. Kết quả đo
Dữ liệu đo được ghi tự động vào các tệp manifest và hồ sơ dữ liệu.

### 3.1 Bảng đặc tính dữ liệu Batch (Nguồn: `docs/evidence/data_profile.md`)
| Đặc tính kiểm định | Giá trị đo được | Quy chuẩn Rubric | Đánh giá |
| :--- | :--- | :--- | :--- |
| Tổng số dòng (Part 1 + Part 2) | 10,200 dòng (chế độ small) | Khớp manifest | Đạt |
| Dung lượng tải lên MinIO | ~1.31 MB (small) / 5.4 GB (full) | Tải lên MinIO | Đạt |
| Tỉ lệ trùng lặp (Duplicate rate) | 2.00% (200 dòng lặp) | ~2.0% | Đạt |
| Lược đồ Part 1 | 9 cột (chưa có discount_percent) | Schema evolution | Đạt |
| Lược đồ Part 2 | 10 cột (có discount_percent) | Schema evolution | Đạt |
| Số lượng user_id duy nhất | 8,924 (tỉ lệ unique 0.875) | High cardinality | Đạt |
| Cột lệch khóa tự nhiên | `event_type=view` chiếm 96.2% | Phân phối thực tế | Đạt |

### 3.2 Bảng đặc tính dữ liệu Stream (Nguồn: `docs/evidence/stream_profile.md`)
| Chỉ số luồng | Giá trị đo được | Mục đích kiểm thử |
| :--- | :--- | :--- |
| Tỉ lệ bản ghi đến muộn (Late arrival) | 5.12% | Kiểm thử Watermark và độ trễ 15 phút của Flink |
| Khoảng thời gian trễ | 300 - 600 giây (5 - 10 phút) | Nằm trong ngưỡng Watermark cho phép |
| Tỉ lệ bản ghi gửi trùng | 1.48% | Kiểm thử khử trùng lặp qua Top-1 Row Number trong Flink |
| Lưu lượng cơ sở | 100 sự kiện/giây | Tải thông thường |
| Lưu lượng đột biến (Burst) | 1,000 sự kiện/giây | Kích hoạt Backpressure trong Flink UI |

## 4. Minh chứng
Các ảnh chụp màn hình ghi nhận kết quả sinh dữ liệu và tải lên dịch vụ lưu trữ:

![Minh chứng Generator Batch Summary](screenshots/E01_generator_batch_summary.png)
*Ảnh chứng minh: Terminal sau khi chạy make gen-data hiển thị số dòng Part 1, Part 2, số duplicate tiêm vào.*

![Minh chứng MinIO Raw Bucket](screenshots/E02_minio_raw_bucket.png)
*Ảnh chứng minh: MinIO Browser trong bucket ecommerce-raw/batch/ chứa 2 file raw_events_old.csv và raw_events_new.csv.*

![Minh chứng Data Profile Summary](screenshots/E03_data_profile_summary.png)
*Ảnh chứng minh: Terminal sau khi chạy make profile-data thể hiện tỉ lệ skew, duplicate rate và cardinality.*

![Minh chứng Schema Evolution Proof](screenshots/E04_schema_evolution_proof.png)
*Ảnh chứng minh: Output test kiểm thử tự động so sánh 9 cột Part 1 và 10 cột Part 2.*

![Minh chứng Stream Generator Stats](screenshots/E05_stream_generator_stats.png)
*Ảnh chứng minh: Log manifest của stream generator hiển thị số sự kiện gửi, tỉ lệ duplicate và phân phối độ trễ.*

## 5. Hạn chế và lưu ý
1. Dữ liệu chế độ `small` chỉ gồm 10,200 dòng phục vụ chạy nhanh trên máy phát triển cá nhân. Khi cần đo đạc hiệu năng với dữ liệu lớn hơn, sử dụng `make gen-data-medium` (5 GB) hoặc `make gen-data-full` (100 GB trên cụm máy chủ đủ đĩa trống).
2. Việc tiêm lệch khóa nhân tạo qua cờ `--skewed` là tùy chọn, dữ liệu REES46 đã có sẵn độ lệch tự nhiên cao ở trường loại sự kiện (`event_type`).
