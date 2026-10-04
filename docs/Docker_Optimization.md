# Bao Cao Toi Uu Hoa Docker Image (Engineering Fundamentals)

> Hang muc rubric: Engineering Fundamentals (Dockerfile optimization, multistage build, layer size comparison, 5.0 diem).  
> Code: [docker/Dockerfile.airflow](../docker/Dockerfile.airflow), [docker/Dockerfile.airflow.single-stage](../docker/Dockerfile.airflow.single-stage), [docker/Dockerfile.airflow.baseline](../docker/Dockerfile.airflow.baseline).  
> Cach chay lai: `make docker-size`.

---

## 1. Vande Can Giai Quyet

Khi xay dung container image tich hop ca Apache Airflow 2.7.3, PySpark 3.5.0 (Delta Lake), va Feast 0.38, image de bi phinh to den 5.5 GB neu de cau hinh mac dinh. Nguyen nhan chu yeu den tu:
1. Cai dat goi day du OpenJDK JDK (chua ca trinh bien dich javac va cong cu debug khong can thiet cho runtime).
2. Tich luy bo nho dem APT deb va pip wheel cache ben trong cac layer container.
3. Thieu tach biet giua qua trinh build va moi truong chay thuc te.

---

## 2. Cach Lam

De danh gia dong gop doc lap cua tung ky thuat, he thong xay dung va do luong 3 bien the Dockerfile:

1. **Bien the 1: Baseline (`docker/Dockerfile.airflow.baseline`)**:
   - Cai dat `openjdk-11-jdk` day du.
   - Khong dung `--no-install-recommends`, khong don dep `/var/lib/apt/lists/*`.
   - Cai dat pip khong co co `--no-cache-dir`.

2. **Bien the 2: Single-Stage Clean (`docker/Dockerfile.airflow.single-stage`)**:
   - Thay the bang `openjdk-11-jre-headless` va `procps`.
   - Bat co `--no-install-recommends` va xoa sach cache apt trong cung 1 lenh `RUN`.
   - Cai dat pip voi co `--no-cache-dir`.

3. **Bien the 3: Multistage Build (`docker/Dockerfile.airflow`)**:
   - *Stage 1 (Builder)*: Cai dat dependencies tu `requirements-airflow.txt` vao `/home/airflow/.local`.
   - *Stage 2 (Runtime)*: Chi sao chep thu muc `/home/airflow/.local` sang container moi co san OpenJDK JRE Headless.

---

## 3. Ket Qua Do

Nguon: [docs/evidence/docker_sizes.txt](evidence/docker_sizes.txt) (Do truc tiep bang Docker Engine):

### 3.1. Bang so sanh tong the 3 bien the

| Bien the | Docker CLI Size | Inspect Bytes | Giam so voi Baseline | Ky thuat chinh |
| :--- | :--- | :--- | :--- | :--- |
| **`ecom-airflow-spark:baseline`** | **5.47 GB** | 1,815,600,939 B | Baseline (0%) | Full JDK 11 + APT cache + Pip cache |
| **`ecom-airflow-spark:single-stage`** | **3.52 GB** | 910,910,659 B | **-862.8 MB (-49.8%)** | Headless JRE + APT purge + pip `--no-cache-dir` |
| **`ecom-airflow-spark:2.7.3` (Multistage)** | **3.52 GB** | 910,903,918 B | **-862.8 MB (-49.8%)** | Builder pattern copy `/home/airflow/.local` |

### 3.2. Phan ra chi tiet dong gop tung layer (Layer History Breakdown)

1. **Layer Java Runtime & OS Packages**:
   - Baseline (`openjdk-11-jdk` + giu cache apt): **511 MB**.
   - Single-stage va Multistage (`openjdk-11-jre-headless` + don apt): **211 MB**.
   - *Muc tiet kiem*: **300 MB** o tang he dieu hanh nho loai bo compiler va GUI packages.
2. **Layer Python Packages**:
   - Baseline (`pip install` khong co `--no-cache-dir`): **1.47 GB** (chua toan bo wheel cache va source archive).
   - Single-stage va Multistage (chi giu installed packages): **720 MB**.
   - *Muc tiet kiem*: **750 MB** nho loai bo bo nho dem pip.

---

## 4. Minh Chung

Minh chung duoc ghi nhan tu ket qua terminal:

![Minh chứng đo lường Docker Sizes](screenshots/E06_docker_size_comparison.png)
*Ảnh chứng minh: Kết quả đo lường thực tế 3 biến thể Docker image, mức giảm 1.95 GB giữa Baseline (5.47GB) và bản Tối ưu (3.52GB).*

---

## 5. Han Che va Luu Y (Giai Trinh Trung Thuc)

1. **Ve su tuong dong giua Single-Stage Clean va Multistage**:
   - Ca hai bien the `single-stage` va `multistage` deu dat dung luong 3.52 GB (chenh lech chi vai KB do metadata buildkit).
   - *Nguyen nhan*: Builder stage su dung cung base image `apache/airflow:2.7.3-python3.10` va khong cai them trinh bien dich C (gcc, make) can duoc loai bo. Vi vay, thu muc `/home/airflow/.local` tao boi builder co kich thuoc giong het single-stage khi chay `pip install --no-cache-dir`.
   - *Danh gia kien truc*: Mau Multistage duoc giu lai vi muc dich tach biet kien truc (Builder Pattern) va chuan hoa quy trinh CI/CD, giup de dang mo rong neu sau nay can them trinh bien dich C cho cac thu vien native.
2. **Kha nang thu nho them**:
   - De thu nho hon nua (< 3 GB), can chuyen sang base image custom slim hoac loai bo mot so dependencies khong dung cua Feast/Airflow. Tuy nhien, viec giu 3.52 GB la can bang toi uu de he thong van chay day du Spark, Delta Lake, va Feast.
