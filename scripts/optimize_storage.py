#!/usr/bin/env python3
"""
================================================================================
SCRIPTS/OPTIMIZE_STORAGE.PY - DELTA LAKEHOUSE STORAGE OPTIMIZATION BENCHMARK
Project: E-Commerce Real-Time Purchase Propensity Prediction System
Author: Hoang Minh Nhan

Purpose:
  Evaluates Lakehouse Compaction, Z-Ordering, and VACUUM performance:
  1. Measures active Parquet files and query time before optimization.
  2. Runs OPTIMIZE ZORDER BY (user_id) to activate data skipping.
  3. Measures reduction in active Parquet files (DESCRIBE DETAIL numFiles).
  4. Runs safe VACUUM (RETAIN 168 HOURS) by default; only runs RETAIN 0 when
     --demo-vacuum is explicitly specified (with time travel warning).
  5. Outputs docs/evidence/lakehouse_inspection.txt.
================================================================================
"""

import os
import sys
import time
import argparse
import subprocess
from datetime import datetime, timezone
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def get_git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("utf-8").strip()
    except Exception:
        return "unknown"


def create_spark_session():
    """Initialize SparkSession for storage optimization."""
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    if "ecom_minio" in endpoint:
        endpoint = endpoint.replace("ecom_minio", "localhost")

    access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID") or "minioadmin"
    secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY") or "minioadmin"

    spark = (
        SparkSession.builder.appName("Storage-Optimization-Benchmark")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def benchmark_storage(demo_vacuum: bool = False):
    spark = create_spark_session()

    minio_lakehouse = "s3a://ecommerce-lakehouse"
    gold_fact_path = f"{minio_lakehouse}/gold/fact_user_events"
    gold_feat_path = f"{minio_lakehouse}/gold/feat_user_30d"

    sample_user_id = 513359812

    print("\n" + "=" * 80)
    print("BAT DAU DO LUONG TRUOC TOI UU (BASELINE / BEFORE)")
    print("=" * 80)

    # 1. Đo lường trước tối ưu
    try:
        detail_before = spark.sql(f"DESCRIBE DETAIL delta.`{gold_fact_path}`").collect()[0]
        files_before = detail_before["numFiles"]
        size_mb_before = detail_before["sizeInBytes"] / (1024 * 1024)
        avg_file_size_before = size_mb_before / files_before if files_before > 0 else 0
    except Exception as e:
        print(f"Loi doc metadata ban dau tu {gold_fact_path}: {e}")
        spark.stop()
        return

    print("Bang fact_user_events:")
    print(f"  - So luong file Parquet hien tai : {files_before} files (phan manh nho)")
    print(f"  - Tong dung luong               : {size_mb_before:.2f} MB")
    print(f"  - Kich thuoc trung binh moi file : {avg_file_size_before:.2f} MB/file")

    print(f"\nDang chay truy van thu nghiem: SELECT COUNT(*) WHERE user_id = {sample_user_id}...")
    start_q_before = time.time()
    try:
        count_res_before = spark.sql(
            f"SELECT COUNT(*) FROM delta.`{gold_fact_path}` WHERE user_id = {sample_user_id}"
        ).collect()[0][0]
    except Exception:
        count_res_before = 0
    time_q_before = time.time() - start_q_before
    print(f"  -> Ket qua: Tim thay {count_res_before} su kien trong {time_q_before:.3f} giay.")

    # 2. Thực thi Compaction & Z-Order
    print("\n" + "=" * 80)
    print("DANG THUC HIEN OPTIMIZE & ZORDER BY (user_id)...")
    print("=" * 80)

    t_opt_start = time.time()
    spark.sql(f"OPTIMIZE delta.`{gold_fact_path}` ZORDER BY (user_id)").collect()
    opt_duration = time.time() - t_opt_start

    spark.sql(f"OPTIMIZE delta.`{gold_feat_path}` ZORDER BY (user_id)").collect()

    # 3. VACUUM
    if demo_vacuum:
        print("\nCANH BAO: Che do --demo-vacuum dang xoa file cu ngay lap tuc (RETAIN 0). Luu y: thao tac nay lam mat Delta Time Travel!")
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        spark.sql(f"VACUUM delta.`{gold_fact_path}` RETAIN 0 HOURS")
        spark.sql(f"VACUUM delta.`{gold_feat_path}` RETAIN 0 HOURS")
    else:
        retain_hours = int(os.getenv("DELTA_VACUUM_RETAIN_HOURS", "168"))
        print(f"\nThuc thi VACUUM an toan (RETAIN {retain_hours} HOURS, bao toan Delta Time Travel)...")
        spark.sql(f"VACUUM delta.`{gold_fact_path}` RETAIN {retain_hours} HOURS")
        spark.sql(f"VACUUM delta.`{gold_feat_path}` RETAIN {retain_hours} HOURS")

    # 4. Đo lường sau tối ưu
    print("\n" + "=" * 80)
    print("KET QUA DO LUONG SAU TOI UU (OPTIMIZED / AFTER)")
    print("=" * 80)

    detail_after = spark.sql(f"DESCRIBE DETAIL delta.`{gold_fact_path}`").collect()[0]
    files_after = detail_after["numFiles"]
    size_mb_after = detail_after["sizeInBytes"] / (1024 * 1024)
    avg_file_size_after = size_mb_after / files_after if files_after > 0 else 0

    print("Bang fact_user_events sau khi Optimize:")
    print(f"  - So luong file Parquet con hoat dong: {files_after} files (da gom)")
    print(f"  - Kich thuoc trung binh moi file     : {avg_file_size_after:.2f} MB/file")

    start_q_after = time.time()
    count_res_after = spark.sql(
        f"SELECT COUNT(*) FROM delta.`{gold_fact_path}` WHERE user_id = {sample_user_id}"
    ).collect()[0][0]
    time_q_after = time.time() - start_q_after
    print(f"  -> Ket qua: Tim thay {count_res_after} su kien trong {time_q_after:.3f} giay.")

    speedup = (time_q_before / time_q_after) if time_q_after > 0 else 1.0

    git_commit = get_git_commit()
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    vac_hours = 0 if demo_vacuum else 168
    vac_mode = "Demo" if demo_vacuum else "Safe"
    vac_str = f"RETAIN {vac_hours}h ({vac_mode})"
    cmd_flag = " --demo-vacuum" if demo_vacuum else ""

    report_lines = [
        "================================================================================",
        "BAO CAO TOI UU HOA LUU TRU LAKEHOUSE (LAKEHOUSE STORAGE OPTIMIZATION REPORT)",
        "================================================================================",
        f"Thoi diem do luong: {timestamp}",
        f"Lenh thuc thi     : python3 scripts/optimize_storage.py{cmd_flag}",
        f"Git commit hash   : {git_commit}",
        f"Bang muc tieu     : {gold_fact_path}",
        f"User ID kiem tra  : {sample_user_id}",
        "--------------------------------------------------------------------------------",
        f"{'Tieu chi danh gia':<32} | {'Truoc toi uu (Before)':<22} | {'Sau toi uu (After)':<22}",
        "--------------------------------------------------------------------------------",
        f"{'1. So file Parquet hoat dong':<32} | {f'{files_before} files (phan manh)':<22} | {f'{files_after} files (da gom)':<22}",
        f"{'2. Kich thuoc trung binh/file':<32} | {f'{avg_file_size_before:.2f} MB/file':<22} | {f'{avg_file_size_after:.2f} MB/file':<22}",
        f"{'3. Ky thuat sap xep da chieu':<32} | {'Chua sap xep':<22} | {'Z-Order (user_id)':<22}",
        f"{'4. Co che Data Skipping':<32} | {'Khong kich hoat':<22} | {'BAT (Bo qua file khong khop)':<22}",
        f"{'5. Thoi gian query user_id':<32} | {f'{time_q_before:.3f}s':<22} | {f'{time_q_after:.3f}s ({speedup:.1f}x)':<22}",
        f"{'6. Thoi gian chay OPTIMIZE':<32} | {'-':<22} | {f'{opt_duration:.2f}s':<22}",
        f"{'7. Che do VACUUM':<32} | {'-':<22} | {f'{vac_str}':<22}",
        "================================================================================"
    ]

    report_text = "\n".join(report_lines)
    print("\n" + report_text + "\n")

    out_file = "docs/evidence/lakehouse_inspection.txt"
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(report_text + "\n")
    print(f"Da ghi nhan ket qua vao: {out_file}\n")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Lakehouse Storage Optimization Benchmark")
    parser.add_argument("--demo-vacuum", action="store_true", help="Chay VACUUM RETAIN 0 cho demo (lam mat Time Travel)")
    args = parser.parse_args()
    benchmark_storage(demo_vacuum=args.demo_vacuum)


if __name__ == "__main__":
    main()
