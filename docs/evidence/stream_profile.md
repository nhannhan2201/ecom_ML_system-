# Bao Cao Do Luong Du Lieu Streaming (Streaming Profile Evidence)

> **Thoi diem tao**: `2026-10-04 03:20:44 UTC`  
> **Git commit**: `c39009f`  
> **Nguon du lieu**: `data/stream_manifest.json` tu Kafka Stream Generator.

---

## 1. Tong Quan Thong Luong va Quy Mo (Volume & Throughput)

| Chi So | Gia Tri Do Duoc | Ghi Chu |
| :--- | :--- | :--- |
| **Topic Kafka** | `ecommerce_stream_events` | 3 partitions, key=`user_id` |
| **Tong so su kien da gui** | 2,024 messages | Bao gom normal, dup, late |
| **So su kien tieu chuan** | 1,886 messages | Luong binh thuong |
| **Thoi gian chay** | 5.84 giay | - |
| **Thong luong trung binh** | 346.5 messages/giay | - |
| **Event Time cuoi cung** | `2019-10-26 00:14:02 UTC` | Tien trinh event time thuc |

---

## 2. Minh Chung Late Arrival (Loi Streaming 2 - Rubric DE)

| Thong So Do Tre | Gia Tri Thuc Nghiem | Muc Tieu Cau Hinh |
| :--- | :--- | :--- |
| **So su kien bi tre** | 114 events | - |
| **So su kien da giai phong** | 114 events | Theo event-time buffer |
| **Ty le su kien tre** | **5.63%** | ~5.00% |
| **Do tre toi thieu (Min)** | 5.0 phut | >= 5 phut |
| **Do tre trung vi (Median)** | 8.0 phut | 5 - 10 phut |
| **Do tre toi da (Max)** | 10.0 phut | <= 10 phut |
| **Do tre trung binh (Mean)** | 7.48 phut | - |

---

## 3. Minh Chung Duplicate Records (Loi Streaming 3 - Rubric DE)

| Hang Muc | Gia Tri Thuc Nghiem | Muc Tieu Cau Hinh |
| :--- | :--- | :--- |
| **So duplicate tiem vao** | 24 messages | Nhan ban x2 goi tin |
| **Ty le duplicate thuc te** | **1.19%** | ~1.50% |

---

## 4. Minh Chung Burst Traffic (Loi Streaming 1 - Rubric DE)

| Cau Hinh Flash Sale | Gia Tri Thuc Te | Mo Ta |
| :--- | :--- | :--- |
| **He so tang dot bien** | x30 | Tang dot bien throughput |
| **Thoi luong burst** | 600 giay | Chu ky flash sale |
| **So su kien trong pha burst** | 1,000 messages | - |
