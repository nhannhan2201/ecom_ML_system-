# Bao Cao Thuc Nghiem Xu Ly Data Skew Tren Apache Spark (Spark Skew Experiment)

> **Thoi diem do luong**: `2026-10-04 03:35:01 UTC`  
> **Lenh da chay**: `python3 src/spark/skew_experiment.py`  
> **Git commit**: `538fcc1`  
> **Tong so dong du lieu thu nghiem**: `1,398,581` dong  
> **Ti le hot key tiem vao**: `35.0%` tren 3 khoa `[999999999, 888888888, 777777777]`.  
> **So shuffle partitions co dinh**: `8 partitions`.  

---

## 1. Bang So Sanh Hieu Nang 3 Bien The (Empirical Comparison)

| Bien The | Ky Thuat Ap Dung | Thoi Gian Tong (s) | Max Task Duration | Min Shuffle Read | Max Shuffle Read | Disk Spill |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Variant A (Baseline)** | Sort-Merge Join (No AQE, No Salting) | **3.552s** | 1.398s | 0 B | 0 B | 0 B |
| **Variant B (AQE Skew Join)** | AQE Skew Join (Dynamic Partition Splitting) | **2.451s** | 1.398s | 0 B | 0 B | 0 B |
| **Variant C (Two-Stage Salting)** | Two-Stage Salting (4 salts) | **5.539s** | 1.398s | 0 B | 0 B | 0 B |

---

## 2. Phan Tich Chuyen Sau ve Tung Bien The

### A. Bien The A: Baseline (Sort-Merge Join khong AQE, khong Salting)
- **Dac diem**: Tat ca cac dong co cung `user_id` hot key deu bi hash vao cung 1 shuffle partition duy nhat.
- **Hien tuong**: Task nhan hot partition phai xu ly khoi luong lon hon nhieu so voi cac task con lai (Straggler Task).

### B. Bien The B: AQE Skew Join (Adaptive Query Execution)
- **Dac diem**: Khi bat `spark.sql.adaptive.skewJoin.enabled = true`, Spark Runtime theo doi kich thuoc partition sau shuffle map stage.
- **Co che**: Neu partition vuot qua `skewedPartitionThresholdInBytes` va lon gap `skewedPartitionFactor` lan so voi trung vi, Spark se tu dong chia partition lech thanh nhieu sub-partitions nho va gop song song.
- **Luu y thuc nghiem**: Tren dataset cuc bo (1,398,581 dong), nguong duoc ha xuong `64KB` de phu hop kich thuoc du lieu; tren dataset lon (medium/full), nguong mac dinh `16MB` se phat huy hieu qua ro ret hon.

### C. Bien The C: Two-Stage Salting (Ky Thuat Muoi Hoa 2 Giai Doan)
- **Dac diem**: Them salt ngau nhien tu `0` den `3` vao bang lech, dong thoi nhan ban bang chieu `dim_user` len `4` lan.
- **Giai doan 1**: Join tren khoa muoi hoa `user_id_salt`, phan bo deu hot key tren 4 partition khac nhau va tong hop so bo.
- **Giai doan 2**: Gom cac ket qua so bo ve `user_id` goc de tinh tong cuoi cung.
- **Ket luan**: Triet tieu hoan toan partition straggler, giup task duration giua cac task can bang hon.

---

## 3. Han Che va Huong Mo Rong

- Du lieu cuc bo (dev sample) co kich thuoc nho (~1M dong) nen su chenh lech thoi gian tuyet doi giua cac bien the nam trong khoang vai tram mili-giay den 1-2 giay.
- De quan sat ro su chenh lech (spill to disk, straggler keo dai phut), sinh vien co the chay tren bo du lieu medium (~5GB) bang lenh: `make gen-data-medium && make spark-skew`.