#!/usr/bin/env python3
"""
================================================================================
SCRIPTS/INSPECT_LAKEHOUSE.PY - CÔNG CỤ TRUY VẤN VÀ KIỂM TRA MINIO LAKEHOUSE
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System
================================================================================
Công cụ kiểm tra nhanh (chạy bằng PyArrow trong vài giây, không cần khởi động Spark):
- Liệt kê số lượng bản ghi và số cột của tất cả các bảng Bronze, Silver, Gold.
- In schema chi tiết và mẫu dữ liệu (sample rows) của bất kỳ bảng nào.
- Kiểm tra dữ liệu Flink Staging (JSON).

Cách sử dụng:
  python scripts/inspect_lakehouse.py                      # Tổng hợp tất cả bảng
  python scripts/inspect_lakehouse.py --table fact         # Xem chi tiết fact_user_events
  python scripts/inspect_lakehouse.py --table feat30d      # Xem chi tiết feat_user_30d
  python scripts/inspect_lakehouse.py --table labels       # Xem chi tiết user_labels
  python scripts/inspect_lakehouse.py --table staging      # Xem dữ liệu Flink Staging
================================================================================
"""

import sys
import argparse
import json
from pyarrow.fs import S3FileSystem
import pyarrow.dataset as ds
import pyarrow.json as paj

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"

fs = S3FileSystem(
    endpoint_override=S3_ENDPOINT,
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
    scheme="http"
)

TABLE_MAP = {
    "bronze": "ecommerce-lakehouse/bronze/raw_events",
    "silver": "ecommerce-lakehouse/silver/stg_events",
    "product": "ecommerce-lakehouse/gold/dim_product",
    "user": "ecommerce-lakehouse/gold/dim_user",
    "fact": "ecommerce-lakehouse/gold/fact_user_events",
    "feat30d": "ecommerce-lakehouse/gold/feat_user_30d",
    "labels": "ecommerce-lakehouse/gold/user_labels",
    "summary": "ecommerce-lakehouse/gold/category_summary",
}


def get_table_dataset(table_path):
    return ds.dataset(
        table_path,
        filesystem=fs,
        format="parquet",
        partitioning="hive",
        ignore_prefixes=["_", "."]
    )


def summarize_all():
    print("\n" + "=" * 85)
    print(" 📊 TỔNG HỢP TOÀN BỘ CÁC BẢNG TRONG MINIO DATA LAKEHOUSE")
    print("=" * 85)
    print(f" {'Tên bảng (Zone / Name)':<32} | {'Số bản ghi':>12} | {'Số cột':>7} | {'Phân vùng (Partition)':<20}")
    print("-" * 85)

    partitions_info = {
        "bronze": "Không phân vùng",
        "silver": "date",
        "product": "Không phân vùng (SCD2)",
        "user": "Không phân vùng",
        "fact": "date (01, 16, 26)",
        "feat30d": "date (2019-10-26)",
        "labels": "date (2019-10-26)",
        "summary": "Không phân vùng",
    }

    total_rows = 0
    for key, path in TABLE_MAP.items():
        try:
            dataset = get_table_dataset(path)
            table = dataset.to_table()
            rows = table.num_rows
            cols = len(table.schema.names)
            total_rows += rows
            part = partitions_info.get(key, "-")
            short_name = path.replace("ecommerce-lakehouse/", "")
            print(f" {short_name:<32} | {rows:>12,} | {cols:>7} | {part:<20}")
        except Exception as e:
            short_name = path.replace("ecommerce-lakehouse/", "")
            print(f" {short_name:<32} | {'CHƯA TỒN TẠI':>12} | {'-':>7} | Lỗi: {e}")

    # Kiểm tra Flink Staging
    try:
        staging_files = fs.get_file_info(
            S3FileSystem().from_uri("http://minioadmin:minioadmin@localhost:9000/ecommerce-raw/staging/stream_events/")[1]
            if False else ds.dataset("ecommerce-raw/staging/stream_events", filesystem=fs, format="json").files
        )
        staging_count = sum(1 for _ in open_staging_sample(1000000))
        print(f" {'staging/stream_events (Flink)':<32} | {staging_count:>12,} | {10:>7} | JSON (Append-only)")
    except Exception:
        pass

    print("-" * 85)
    print(f" 👉 Tổng số bản ghi quản lý trong Lakehouse: {total_rows:,} records")
    print("=" * 85 + "\n")


def open_staging_sample(limit=5):
    dataset = ds.dataset("ecommerce-raw/staging/stream_events", filesystem=fs, format="json")
    for file in dataset.files:
        with fs.open_input_stream(file) as f:
            for line in f:
                if line.strip():
                    yield json.loads(line.decode("utf-8"))
                    limit -= 1
                    if limit <= 0:
                        return


def inspect_table(key_or_name, limit=5):
    # Tìm bảng theo key viết tắt hoặc tên đầy đủ
    target_path = None
    if key_or_name.lower() in TABLE_MAP:
        target_path = TABLE_MAP[key_or_name.lower()]
    elif key_or_name.lower() in ["staging", "stream_events"]:
        print(f"\n📂 KIỂM TRA FLINK RAW STAGING: s3://ecommerce-raw/staging/stream_events/")
        sample = list(open_staging_sample(limit))
        print(f"🔍 Mẫu {len(sample)} sự kiện thô đầu tiên:")
        for idx, row in enumerate(sample, 1):
            print(f"  [{idx}] {json.dumps(row, ensure_ascii=False)}")
        return
    else:
        for k, p in TABLE_MAP.items():
            if key_or_name.lower() in p.lower():
                target_path = p
                break

    if not target_path:
        print(f"❌ Không tìm thấy bảng: '{key_or_name}'. Các bảng hợp lệ:")
        print("   " + ", ".join(TABLE_MAP.keys()) + ", staging")
        return

    print(f"\n" + "=" * 80)
    print(f" 🔍 CHI TIẾT BẢNG: {target_path}")
    print("=" * 80)
    try:
        dataset = get_table_dataset(target_path)
        table = dataset.to_table()
        print(f"📈 Tổng số bản ghi : {table.num_rows:,}")
        print(f"📋 Tổng số cột     : {len(table.schema.names)}")
        print("\n📝 DANH SÁCH CỘT VÀ KIỂU DỮ LIỆU:")
        for field in table.schema:
            print(f"  - {field.name:<28} : {str(field.type)}")

        print(f"\n📄 MẪU {limit} DÒNG DỮ LIỆU ĐẦU TIÊN:")
        pydict = table.slice(0, limit).to_pydict()
        for i in range(min(limit, table.num_rows)):
            row = {col: pydict[col][i] for col in table.schema.names}
            print(f"\n[Dòng {i+1}]:")
            for col, val in row.items():
                print(f"  {col:<26}: {val}")
        print("=" * 80 + "\n")
    except Exception as e:
        print(f"❌ Lỗi khi đọc bảng: {e}")


def main():
    parser = argparse.ArgumentParser(description="Công cụ kiểm tra nhanh MinIO Delta Lakehouse")
    parser.add_argument("--table", "-t", type=str, default=None, help="Tên bảng cần xem chi tiết (fact, feat30d, labels, product, user, silver, bronze, staging)")
    parser.add_argument("--limit", "-n", type=int, default=3, help="Số dòng mẫu cần hiển thị (mặc định 3)")
    args = parser.parse_args()

    if args.table:
        inspect_table(args.table, args.limit)
    else:
        summarize_all()


if __name__ == "__main__":
    main()
