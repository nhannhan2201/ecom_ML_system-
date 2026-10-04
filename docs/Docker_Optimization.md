# Báo Cáo Tối Ưu Hóa Docker Image (Engineering Fundamentals - 5đ)

> **Mục tiêu rubric**: Thiết kế Dockerfile tối ưu (multistage build pattern, giảm thiểu layer thừa, loại bỏ cache) và cung cấp bảng đo lường dung lượng thực tế trước và sau khi tối ưu.
> **Tệp bằng chứng thô**: `docs/evidence/docker_sizes.txt` (đo trực tiếp bằng Docker Engine trên môi trường chạy).

---

## 1. Phương Pháp So Sánh & Kỹ Thuật Áp Dụng

Để đo lường khách quan và chính xác, dự án thiết lập hai phiên bản Dockerfile:
1. **Bản Baseline (`docker/Dockerfile.airflow.baseline`)**: Cài đặt trực tiếp trên base image `apache/airflow:2.7.3-python3.10`.
   - Cài đặt đầy đủ `openjdk-11-jdk` (bao gồm GUI, X11, Java compiler, công cụ debug).
   - Không áp dụng `--no-install-recommends`.
   - Không dọn dẹp bộ nhớ đệm APT (`/var/lib/apt/lists/*`).
   - Cài đặt thư viện Python trực tiếp không kèm `--no-cache-dir`, giữ lại toàn bộ file wheel đã tải trong cache pip.
2. **Bản Tối Ưu Multistage (`docker/Dockerfile.airflow`)**:
   - **Pattern Multistage Build**:
     - *Stage 1 (Builder)*: Tách riêng bước cài đặt dependencies từ file `docker/requirements-airflow.txt` vào thư mục người dùng `/home/airflow/.local` với cờ `--no-cache-dir`.
     - *Stage 2 (Runtime)*: Chỉ sao chép thành phẩm `/home/airflow/.local` từ Builder sang Runtime container sạch. Toàn bộ công cụ biên dịch tạm thời bị loại bỏ hoàn toàn.
   - **Chỉ dùng Headless JRE**: Thay thế `openjdk-11-jdk` bằng `openjdk-11-jre-headless` kết hợp `procps`, vừa đủ để PySpark driver và JVM tương tác mượt mà.
   - **Tối ưu cờ APT**: Dùng `--no-install-recommends` loại bỏ các gói phụ thuộc không cần thiết.
   - **Dọn dẹp triệt để layer APT**: Thực hiện `apt-get autoremove -yqq --purge && apt-get clean && rm -rf /var/lib/apt/lists/*` trong cùng một lệnh `RUN` để tránh phình dung lượng lưu trữ layer.
   - **File `.dockerignore`**: Loại trừ dữ liệu lớn (`data/`), tài liệu (`docs/`), notebook, tệp tạm `.git`, bytecode `.pyc` khỏi Docker build context.

---

## 2. Bảng Đo Lường Kích Thước Thực Tế (Số Liệu Đo Thật)

Dữ liệu được trích xuất từ lệnh `docker image ls` và `docker history` (xem chi tiết tại `docs/evidence/docker_sizes.txt`):

| Thành Phần / Image | Tag | Image ID | Dung Lượng (Size) | Mức Độ Giảm | Ghi Chú Kỹ Thuật |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Airflow Base Image** | `2.7.3-python3.10` | `19586e24db10` | **2.08 GB** | - | Image nền chính thức của Apache Airflow |
| **Baseline Airflow** | `baseline` | `3325e53c7fac` | **5.47 GB** | Baseline (0%) | Full JDK 11 + APT cache + Pip cache tích lũy |
| **Optimized Airflow (Multistage)** | `2.7.3` | `dfcc306fdbe8` | **3.52 GB** | **-1.95 GB (-35.6%)** | Multistage + Headless JRE + no-cache |

### Phân rã chi tiết từng layer (Layer History Breakdown):

1. **Layer Hệ điều hành & Java Runtime**:
   - Baseline (`openjdk-11-jdk` + không clean cache): **511 MB**.
   - Optimized (`openjdk-11-jre-headless` + `--no-install-recommends` + clean cache): **269 MB**.
   - **Mức giảm**: Giảm **242 MB** (tiết kiệm **47.3%** kích thước layer Java).
2. **Layer Thư viện Python**:
   - Baseline (`pip install` thông thường, lưu trữ `.cache/pip`): **1.47 GB**.
   - Optimized (`COPY --from=builder /home/airflow/.local`): **1.17 GB**.
   - **Mức giảm**: Giảm **300 MB** nhờ loại bỏ wheel cache và build artifacts.
3. **Tổng dung lượng tiết kiệm trên toàn bộ Image**:
   - Giảm tổng cộng **1.95 GB** (từ **5.47 GB** xuống **3.52 GB**).
   - Tốc độ pull/push image qua mạng nội bộ và thời gian khởi tạo container trên môi trường CI/CD được cải thiện đáng kể.

---

## 3. Xác Thực Độ Tương Thích & Tính Hoạt Động Của Hệ Thống

Sau khi build hoàn tất image tối ưu `ecom-airflow-spark:2.7.3`, chúng tôi đã khởi chạy kiểm tra độc lập và xác nhận:
1. **Biên dịch và nạp DAG**: Kiểm tra toàn bộ DAG trong pipeline (`dp1_ingest_stream_to_bronze`, `dp2_bronze_to_silver_and_gold`, `dp3_silver_to_postgres_dwh`, `dp4_feast_materialize`) bên trong container image mới đều import thành công (`Import OK`), không phát sinh lỗi thiếu thư viện PySpark hay Feast.
2. **Java Runtime Engine**: Biến môi trường `JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64` và binary `java` hoạt động chính xác với PySpark 3.5.0.

Lệnh tái tạo kết quả:
```bash
make docker-size
```
