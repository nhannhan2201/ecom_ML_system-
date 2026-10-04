# Báo Cáo Thiết Kế Lược Đồ Dữ Liệu (Schema Design)
> Hạng mục rubric: Documentation - Schema Design: Visualize all zones (2.0đ), Dim table SCD2 (2.0đ), Feature tables (2.0đ), Dim & Fact relationship (2.0đ), Naming convention (2.0đ) = 10.0 điểm. Mã nguồn: [setup_dwh_schemas.py](file:///home/nhan/Projects/ecom_ML_system/scripts/setup_dwh_schemas.py), [spark_optimized.py](file:///home/nhan/Projects/ecom_ML_system/src/spark/spark_optimized.py), [features.py](file:///home/nhan/Projects/ecom_ML_system/feature_store/features.py).

## 1. Vấn đề cần giải quyết
Hệ thống dữ liệu thương mại điện tử cần phục vụ đồng thời hai mục đích: phân tích kinh doanh (BI/Reporting) và huấn luyện, phục vụ mô hình học máy (ML Feature Serving). Việc thiết kế lược đồ phải giải quyết được bài toán lưu vết lịch sử biến động giá của sản phẩm theo thời gian (SCD Type 2), phân tách rõ ràng giữa sự kiện giao dịch và bảng chiều, đảm bảo tính toàn vẹn khóa ngoại, và cung cấp các trường thời gian chuẩn xác để chống rò rỉ dữ liệu (data leakage) khi tính toán đặc trưng.

## 2. Cách làm
Dự án thiết kế cấu trúc dữ liệu đa tầng theo mô hình Medallion trên MinIO Delta Lake và Kimball Star Schema trên PostgreSQL DWH:

### 2.1 Phân tầng Medallion và Quy ước đặt tên (Naming Convention)
Toàn bộ bảng và tập tin trong hệ thống tuân thủ quy tắc tiền tố nhất quán:
- **Tầng Bronze (`raw_`)**: Dữ liệu thô nguyên bản (`raw_events`), lưu trữ dạng Delta Lake tại `ecommerce-lakehouse/bronze/raw_events`.
- **Tầng Silver (`stg_`)**: Dữ liệu sạch đã khử trùng lặp (`stg_events`), lưu trữ tại `ecommerce-lakehouse/silver/stg_events`.
- **Tầng Gold DWH (`dim_`, `fact_`)**: Mô hình hình sao Star Schema trên schema `gold` của PostgreSQL:
  - Bảng chiều: `dim_product`, `dim_user`.
  - Bảng sự kiện: `fact_user_events`.
- **Tầng Feature Store (`feat_`)**: Bảng đặc trưng phục vụ Feast: `feat_user_30d` (offline batch) và `feat_user_stream` (online stream).

### 2.2 Bảng chiều biến đổi chậm SCD Type 2 (`dim_product`)
- **Khái niệm SCD Type 2**: Kỹ thuật lưu trữ cho phép theo dõi toàn bộ lịch sử thay đổi của một thực thể bằng cách tạo một dòng mới mỗi khi có thuộc tính thay đổi, thay vì ghi đè lên dòng cũ.
- **Cấu trúc trường bắt buộc**:
  - `product_sk`: Khóa thay thế duy nhất (Surrogate Key) sinh ra từ hàm băm MD5 của `product_id`, `price` và ngày hiệu lực.
  - `product_id`: Khóa tự nhiên của sản phẩm (Natural Key).
  - `valid_from_ts`: Mốc thời gian bắt đầu có hiệu lực của mức giá/thuộc tính.
  - `valid_to_ts`: Mốc thời gian hết hiệu lực (mặc định '9999-12-31' cho phiên bản hiện hành).
  - `is_current`: Cờ logic (`TRUE` nếu là phiên bản đang áp dụng, `FALSE` nếu là lịch sử cũ).

### 2.3 Bảng đặc trưng Feature Store (`feat_user_30d`)
- Bảng đặc trưng tuân thủ nghiêm ngặt chuẩn Feast Entity & Feature View, bắt buộc chứa 2 cột thời gian:
  - `event_timestamp`: Mốc thời gian sự kiện logic dùng để join point-in-time chính xác, chống rò rỉ thông tin tương lai.
  - `created`: Mốc thời gian dòng đặc trưng được tính toán và ghi nhận vào cơ sở dữ liệu.
- Chứa các đặc trưng RFM tính toán trong 30 ngày: `f_user_view_count_30d`, `f_user_cart_count_30d`, `f_user_purchase_count_30d`, `f_user_total_spend_30d`, `f_user_cart_to_view_ratio_30d`.

### 2.4 Quan hệ giữa Dim và Fact (Kimball Star Schema)
- Bảng `gold.fact_user_events` là bảng Pure Fact (loại bỏ thuộc tính sản phẩm tĩnh, chỉ lưu các khóa và độ đo sự kiện).
- Khóa ngoại liên kết chặt chẽ:
  - `fact_user_events.product_sk` -> `dim_product.product_sk` (quan hệ N:1)
  - `fact_user_events.user_id` -> `dim_user.user_id` (quan hệ N:1)

## 3. Kết quả đo
Thống kê cấu trúc các bảng qua script kiểm tra tự động:

| Tầng dữ liệu | Tên bảng | Tiền tố | Khóa chính / Khóa ngoại | Mục đích sử dụng |
| :--- | :--- | :--- | :--- | :--- |
| Bronze | `raw_events` | `raw_` | Không có (Append-only) | Lưu trữ thô bất biến |
| Silver | `stg_events` | `stg_` | Khóa logic 4 trường | Dữ liệu sạch không trùng lặp |
| Gold | `dim_product` | `dim_` | `product_sk` (PK) | SCD Type 2 lưu lịch sử giá |
| Gold | `dim_user` | `dim_` | `user_id` (PK) | Hồ sơ tổng hợp khách hàng |
| Gold | `fact_user_events` | `fact_` | `event_id` (PK), `product_sk` (FK) | Bảng sự kiện hành vi |
| Gold | `feat_user_30d` | `feat_` | `user_id`, `event_timestamp` | Feast Batch Feature View |
| Gold | `user_labels` | Nhãn ML | `user_id`, `event_timestamp` | Nhãn nhị phân mua hàng [0, 1] |

## 4. Minh chứng
![Minh chứng ERD All Zones](screenshots/E25_schema_erd_all_zones.png)
*Ảnh chứng minh: Sơ đồ ERD các bảng trong Database ecom_dwh qua 3 schemas bronze, silver, gold.*

![Minh chứng Dim Product SCD Type 2](screenshots/E26_scd2_dim_product.png)
*Ảnh chứng minh: Dữ liệu mẫu bảng dim_product với các trường valid_from_ts, valid_to_ts, is_current.*

![Minh chứng Feast Feature View Schema](screenshots/E27_feat_table_schema.png)
*Ảnh chứng minh: Cấu trúc bảng feat_user_30d có đủ 2 cột bắt buộc event_timestamp và created.*

![Minh chứng Fact Dim Relationships](screenshots/E28_fact_dim_relationship.png)
*Ảnh chứng minh: Kết quả truy vấn INNER JOIN fact_user_events với các bảng Dim khớp 100% không mất bản ghi.*

## 5. Hạn chế và lưu ý
1. Việc duy trì SCD Type 2 yêu cầu tính toán Surrogate Key (`product_sk`) cẩn thận tại bước nạp dữ liệu Spark để tránh trùng lặp bản ghi hiện hành.
2. Các bảng Feature được xuất ra tệp Parquet sạch tại `s3://ecommerce-lakehouse/feast/` để Feast đọc trực tiếp mà không bị ảnh hưởng bởi thư mục siêu dữ liệu `_delta_log`.
