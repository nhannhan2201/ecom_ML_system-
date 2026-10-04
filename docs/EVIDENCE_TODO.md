# CHECKLIST MINH CHỨNG HÌNH ẢNH CẦN CHỤP BỔ SUNG (EVIDENCE TODO)

> **Mục đích**: Bảng phân công các ảnh chụp màn hình UI thực tế cần chụp bằng tay khi khởi chạy cụm dịch vụ, phục vụ hoàn thiện 100% bằng chứng trực quan cho buổi bảo vệ đồ án theo Rubric EDAI K11.
> **Quy tắc**: Tuyệt đối không tự tạo hoặc giả mạo ảnh chụp. Mỗi ảnh phải có mục tiêu minh chứng rõ ràng và mô tả nghiệp vụ cụ thể.

---

## Danh Sách Checklist Chi Tiết

| STT | Hạng mục Rubric | URL / Cổng truy cập | Nội dung cần chụp & Mục đích minh chứng | Tên file đề xuất (`docs/screenshots/`) | Tài liệu nhúng ảnh | Trạng thái |
| :---: | :--- | :--- | :--- | :--- | :--- | :---: |
| 1 | **Batch Processing (Spark Optimization)** | `http://localhost:4040` (Spark UI khi chạy Spark Job) | **Spark UI Stages - Optimized Job**: Chụp tab *Stages* và *SQL/Dataframe* sau khi chạy `make spark-opt`. Minh chứng tính năng AQE tự động coalesce shuffle partitions, Broadcast Hash Join loại bỏ shuffle join, và chỉ số Spill (Memory/Disk) = 0. | `26_spark_optimized_stages_aqe.png` | `docs/Spark_Optimization_Report.md` | `NEEDS-MANUAL-SCREENSHOT` |
| 2 | **Data Governance (Lineage End-to-End)** | `http://localhost:9002` (DataHub UI, đăng nhập `datahub`/`datahub`) | **DataHub Lineage Graph**: Mở dataset `gold.fact_user_events` hoặc Pipeline `airflow_dp2`, chọn tab **Lineage**. Chụp toàn bộ đồ thị luồng đi từ `ecommerce-raw` $\rightarrow$ Bronze $\rightarrow$ Silver $\rightarrow$ Gold và sang Data Jobs. | `27_datahub_lineage_graph.png` | `docs/Data_Governance_Report.md`, `docs/DATAHUB.md` | `NEEDS-MANUAL-SCREENSHOT` |
| 3 | **Data Governance (Schema Contracts)** | `http://localhost:9002` (DataHub UI) | **DataHub Contracts & Validation Tab**: Mở dataset `gold_dim_product` hoặc `gold_user_labels`, chọn tab **Schema** và **Quality/Contracts**. Chụp bảng schema có gán tags SLA và trạng thái contract *Active/Passing*. | `28_datahub_schema_contracts.png` | `docs/Data_Governance_Report.md` | `NEEDS-MANUAL-SCREENSHOT` |
| 4 | **Data Storage Optimization (Compaction)** | `http://localhost:9001` (MinIO Console, user `minioadmin`) | **MinIO Browser - Compacted Partitions**: Mở bucket `ecommerce-lakehouse/gold/fact_user_events/`. Chụp cây thư mục các phân vùng `date=2019-10-01/`, `date=2019-10-16/`, `date=2019-10-26/` mỗi phân vùng chỉ chứa 1 file Parquet kích thước lớn (40MB+) thay vì nhiều file nhỏ. | `29_minio_compacted_partitions.png` | `docs/Data_Storage_Optimization.md` | `NEEDS-MANUAL-SCREENSHOT` |
| 5 | **Star Schema DWH ERD (Xác nhận)** | DBeaver kết nối `localhost:5432` / `ecom_dwh` | **ERD Diagram Toàn Vẹn**: Mở schema `gold`, chọn *View Diagram*. Kiểm tra 5 bảng (`dim_user`, `dim_product`, `fact_user_events`, `feat_user_30d`, `user_labels`) có đủ đường nối quan hệ khóa ngoại FK. (Ảnh `18_dwh_star_schema_erd.png` đã có sẵn, chỉ cần xác nhận lại tính cập nhật). | `18_dwh_star_schema_erd.png` | `docs/Schema_Design.md` | `VERIFIED` |

---

## Hướng Dẫn Thao Tác Chụp & Lưu Trữ

1. Khởi động các stack cần chụp:
   ```bash
   make up-infra up-airflow up-datahub
   ```
2. Thực thi lệnh tương ứng để sinh trạng thái UI:
   - Với DataHub: `make governance-sync` rồi mở `http://localhost:9002`.
   - Với Spark UI: chạy `make spark-opt` và chụp tab Jobs/Stages trong lúc hoặc ngay sau khi job hoàn thành.
   - Với MinIO: mở `http://localhost:9001` và duyệt vào bucket `ecommerce-lakehouse`.
3. Lưu ảnh trực tiếp vào `docs/screenshots/<tên_file_ở_bảng_trên>.png`.
4. Tài liệu tương ứng đã có sẵn vị trí nhúng thẻ Markdown:
   ```markdown
   ![Mô tả ảnh](screenshots/16_spark_baseline_stage14_skew.png)
   *Mục đích minh chứng: <mô tả ngắn gọn theo bảng trên>*
   ```

