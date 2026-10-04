# Báo Cáo Quản Trị Dữ Liệu DataHub (Data Governance Report)
> Hạng mục rubric: Data Governance: DP1 Lineage & Validation (4.0đ), DP2 Lineage & Validation (4.0đ), DP3 Lineage & Validation (4.0đ) = 12.0 điểm. Mã nguồn: [governance/](file:///home/nhan/Projects/ecom_ML_system/governance/). Cách chạy lại: `make governance-sync` và `make governance-verify`.

## 1. Vấn đề cần giải quyết
Trong một hệ sinh thái dữ liệu phức tạp gồm nhiều tầng xử lý (Lakehouse, DWH, Feature Store), người vận hành và chuyên viên phân tích thường gặp khó khăn trong việc xác định nguồn gốc dữ liệu (Lineage), khó phát hiện các bảng bị lỗi hoặc thay đổi cấu trúc, và thiếu cơ chế theo dõi tập trung mức độ tin cậy của các tập dữ liệu. Cần có một nền tảng siêu dữ liệu (Metadata Platform) để trực quan hóa phả hệ và giám sát các hợp đồng chất lượng dữ liệu.

## 2. Cách làm
Dự án sử dụng Acryl DataHub triển khai theo cơ chế khai báo tập trung (Declarative Governance):

### 2.1 Định nghĩa danh mục siêu dữ liệu tập trung (`catalog.py`)
- Toàn bộ 12 tập dữ liệu (Datasets) thuộc các vùng Raw, Bronze, Silver, Gold DWH, và Feature Store được định nghĩa bằng mã nguồn Python tại `governance/catalog.py`.
- Mỗi dataset có đầy đủ lược đồ kiểu dữ liệu (`SchemaFieldSpec`), quyền sở hữu (ownership), nhãn phân loại (tags: `Bronze`, `Silver`, `Gold`, `SCD2`, `Feast`) và miền nghiệp vụ (domain).

### 2.2 Đồng bộ phả hệ luồng dữ liệu (`sync_catalog.py`)
- Script `governance/sync_catalog.py` chuyển đổi định nghĩa danh mục thành các bản tin Metadata Change Proposal (MCP) và phát qua REST API tới DataHub GMS (`http://localhost:8089` hoặc `http://datahub-gms:8080`).
- Thiết lập phả hệ liên kết hai chiều giữa các pipeline Airflow và các bảng dữ liệu:
  - DP1 liên kết: `ecommerce-raw/batch` + `ecommerce-raw/staging` -> `bronze/raw_events`
  - DP2 liên kết: `bronze/raw_events` -> `silver/stg_events` -> `dim_product`, `dim_user`, `fact_user_events`
  - DP3 liên kết: `silver/stg_events` -> `feat_user_30d` + `user_labels`

### 2.3 Kiểm định hợp đồng dữ liệu độc lập (`verify_contracts.py`)
- Script `governance/verify_contracts.py` tải trực tiếp mẫu dữ liệu thực tế từ MinIO S3 bằng PyArrow, tính toán các hàm thuần và đối chiếu với các hợp đồng chất lượng:
  - Bronze: Không được chứa giá trị null ở trường `user_id`.
  - Silver: Không được trùng lặp theo bộ khóa logic `(user_id, event_time, product_id, event_type)`.
  - Gold SCD2: `product_sk` không null, thời gian `valid_from_ts <= valid_to_ts`, mỗi sản phẩm chỉ có đúng một bản ghi hiện hành `is_current = True`.
  - Gold Features: Bắt buộc có hai trường `event_timestamp` và `created`.
  - Gold Labels: Giá trị nhãn bắt buộc thuộc tập nhị phân [0, 1].
- Kết quả kiểm định được gửi trực tiếp lên DataHub dưới dạng `AssertionRunEvent` để hiển thị trên giao diện người dùng.

## 3. Kết quả đo
Kết quả kiểm tra hợp đồng chất lượng dữ liệu qua `make governance-verify`:

| Tập dữ liệu kiểm định | Loại hợp đồng (Assertion) | Ngưỡng kỳ vọng | Giá trị quan sát | Kết quả |
| :--- | :--- | :--- | :--- | :--- |
| `bronze/raw_events` | Tỉ lệ trường `user_id` rỗng (null) | 0.0% | 0.0% (0 bản ghi null) | PASSED |
| `silver/stg_events` | Tỉ lệ bản ghi trùng lặp khóa logic | 0.0% | 0.0% (đã khử 100%) | PASSED |
| `gold/dim_product` | Tính toàn vẹn SCD2 và khóa duy nhất | 100% | 100% hợp lệ | PASSED |
| `gold/feat_user_30d` | Có cột thời gian chuẩn Feast | Bắt buộc | Đủ 2 cột timestamp | PASSED |
| `gold/user_labels` | Phân phối nhãn nhị phân | {0, 1} | Chỉ chứa 0 và 1 | PASSED |

## 4. Minh chứng
![Minh chứng DataHub Datasets](screenshots/E20_datahub_datasets_list.png)
*Ảnh chứng minh: Danh sách 8 datasets thuộc platform Delta Lake và Postgres trên DataHub Catalog.*

![Minh chứng DataHub Lineage DP1](screenshots/E21_datahub_dp1_lineage.png)
*Ảnh chứng minh: Giao diện DataHub Lineage thể hiện luồng liên kết từ S3 raw batch sang Bronze Delta Lake qua pipeline DP1.*

![Minh chứng DataHub Lineage DP2](screenshots/E22_datahub_dp2_lineage.png)
*Ảnh chứng minh: Đồ thị Lineage từ Bronze sang Silver và rẽ nhánh sang 3 bảng Gold Star Schema qua pipeline DP2.*

![Minh chứng DataHub Lineage DP3](screenshots/E23_datahub_dp3_lineage.png)
*Ảnh chứng minh: Đồ thị Lineage từ Silver sang bảng đặc trưng feat_user_30d và nhãn user_labels qua pipeline DP3.*

![Minh chứng DataHub Assertions Passing](screenshots/E24_datahub_assertions_passed.png)
*Ảnh chứng minh: Tab Validations/Assertions của các bảng Gold hiển thị huy hiệu xanh PASSED.*

## 5. Hạn chế và lưu ý
1. DataHub đóng vai trò giám sát siêu dữ liệu độc lập (out-of-band), việc kiểm định dữ liệu không làm tăng thời gian chạy của các Spark/Flink worker trong luồng dữ liệu chính.
2. Kiểm định hợp đồng lấy mẫu trên 50,000 dòng đầu tiên từ MinIO để đảm bảo tốc độ phản hồi nhanh khi xác thực trên máy phát triển cá nhân.
