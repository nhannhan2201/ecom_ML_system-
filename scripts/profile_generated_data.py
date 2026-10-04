#!/usr/bin/env python3
"""
================================================================================
SCRIPT: PROFILE GENERATED DATA (MEASURE REAL DATA DISTRIBUTIONS & QUALITY)
Project: E-Commerce Real-Time Purchase Propensity Prediction System
Author: Hoang Minh Nhan

Purpose:
  Profiles actual generated batch and streaming data against the 5 Rubric items:
  1. Skewness (event_type, category_code, brand, user_id hot keys)
  2. High-Cardinality (unique counts and unique/total ratio per column)
  3. Schema Evolution (old 9 cols vs new 10 cols with discount_percent)
  4. Duplicate Rate (exact measured duplicate percentages)
  5. Data Volume (exact bytes, row counts, and file sizes)

Integrity Invariant:
  Must verify that total rows in profile match data/generation_manifest.json exactly.
  If row counts mismatch, script exits with an error.

Outputs:
  - docs/evidence/data_profile.json
  - docs/evidence/data_profile.md
================================================================================
"""

import os
import io
import json
import logging
import subprocess
from datetime import datetime, timezone
import pandas as pd
import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [DataProfiler] %(message)s")
logger = logging.getLogger("DataProfiler")


def get_git_commit() -> str:
    """Get current git commit hash."""
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("utf-8").strip()
    except Exception:
        return "unknown"


def load_manifest() -> dict:
    """Load generation manifest from data/generation_manifest.json."""
    manifest_path = "data/generation_manifest.json"
    if not os.path.exists(manifest_path):
        logger.warning(f"Manifest not found at {manifest_path}. Proceeding without strict check.")
        return {}
    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_data_from_minio_or_local():
    """Load Part 1 and Part 2 datasets from MinIO or fallback to local storage."""
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1"
    )

    bucket = "ecommerce-raw"
    p1_key = "batch/raw_events_old.csv"
    p2_key = "batch/raw_events_new.csv"

    df_p1 = None
    df_p2 = None
    p1_size_bytes = 0
    p2_size_bytes = 0

    try:
        resp1 = s3.get_object(Bucket=bucket, Key=p1_key)
        p1_bytes = resp1["Body"].read()
        p1_size_bytes = len(p1_bytes)
        df_p1 = pd.read_csv(io.BytesIO(p1_bytes))
        logger.info(f"Loaded Part 1 from MinIO: {p1_size_bytes:,} bytes, {len(df_p1):,} rows")
    except Exception as e:
        logger.warning(f"Could not load Part 1 from MinIO: {e}")

    try:
        resp2 = s3.get_object(Bucket=bucket, Key=p2_key)
        p2_bytes = resp2["Body"].read()
        p2_size_bytes = len(p2_bytes)
        df_p2 = pd.read_csv(io.BytesIO(p2_bytes))
        logger.info(f"Loaded Part 2 from MinIO: {p2_size_bytes:,} bytes, {len(df_p2):,} rows")
    except Exception as e:
        logger.warning(f"Could not load Part 2 from MinIO: {e}")

    # Fallback to local files
    if df_p1 is None and os.path.exists("data/raw_events_old.csv"):
        df_p1 = pd.read_csv("data/raw_events_old.csv")
        p1_size_bytes = os.path.getsize("data/raw_events_old.csv")
        logger.info(f"Loaded Part 1 from data/raw_events_old.csv: {p1_size_bytes:,} bytes, {len(df_p1):,} rows")

    if df_p2 is None and os.path.exists("data/raw_events_new.csv"):
        df_p2 = pd.read_csv("data/raw_events_new.csv")
        p2_size_bytes = os.path.getsize("data/raw_events_new.csv")
        logger.info(f"Loaded Part 2 from data/raw_events_new.csv: {p2_size_bytes:,} bytes, {len(df_p2):,} rows")

    if df_p1 is None or df_p2 is None:
        raise RuntimeError("Data not found in MinIO or data/ directory. Run 'make gen-data' first.")

    return df_p1, df_p2, p1_size_bytes, p2_size_bytes


def profile_dataset(df_p1: pd.DataFrame, df_p2: pd.DataFrame, p1_bytes: int, p2_bytes: int, manifest: dict) -> dict:
    """Calculate profile metrics covering all 5 Rubric criteria."""
    df_combined = pd.concat([df_p1, df_p2], ignore_index=True)
    total_rows = len(df_combined)
    total_bytes = p1_bytes + p2_bytes

    # Strict integrity assertion against generation manifest
    manifest_rows = manifest.get("total_rows")
    if manifest_rows is not None and total_rows != manifest_rows:
        error_msg = f"Profile row count mismatch: profiled {total_rows} rows but manifest specifies {manifest_rows} rows!"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # 1. Volume
    volume_metrics = {
        "part1_rows": len(df_p1),
        "part1_bytes": p1_bytes,
        "part1_mb": round(p1_bytes / (1024 * 1024), 2),
        "part2_rows": len(df_p2),
        "part2_bytes": p2_bytes,
        "part2_mb": round(p2_bytes / (1024 * 1024), 2),
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "total_mb": round(total_bytes / (1024 * 1024), 2)
    }

    # 2. Schema Evolution
    schema_metrics = {
        "part1_columns": list(df_p1.columns),
        "part1_column_count": df_p1.shape[1],
        "part2_columns": list(df_p2.columns),
        "part2_column_count": df_p2.shape[1],
        "new_columns_in_part2": [c for c in df_p2.columns if c not in df_p1.columns],
        "part1_data_types": {c: str(t) for c, t in df_p1.dtypes.items()},
        "part2_data_types": {c: str(t) for c, t in df_p2.dtypes.items()}
    }

    # 3. Duplicate Rate
    key_cols = [c for c in ["event_time", "event_type", "product_id", "user_id"] if c in df_combined.columns]
    dup_full_p1 = int(df_p1.duplicated().sum())
    dup_full_p2 = int(df_p2.duplicated().sum())
    dup_key_total = int(df_combined.duplicated(subset=key_cols).sum())

    duplicate_metrics = {
        "part1_full_duplicates": dup_full_p1,
        "part1_dup_rate_pct": round((dup_full_p1 / len(df_p1)) * 100, 2) if len(df_p1) > 0 else 0,
        "part2_full_duplicates": dup_full_p2,
        "part2_dup_rate_pct": round((dup_full_p2 / len(df_p2)) * 100, 2) if len(df_p2) > 0 else 0,
        "total_key_duplicates": dup_key_total,
        "total_key_dup_rate_pct": round((dup_key_total / total_rows) * 100, 2) if total_rows > 0 else 0
    }

    # 4. High-Cardinality Analysis
    cardinality_metrics = {}
    for col in df_combined.columns:
        n_unique = int(df_combined[col].nunique(dropna=True))
        n_non_null = int(df_combined[col].count())
        ratio = round(n_unique / n_non_null, 6) if n_non_null > 0 else 0.0
        cardinality_metrics[col] = {
            "unique_count": n_unique,
            "non_null_count": n_non_null,
            "cardinality_ratio": ratio,
            "is_high_cardinality": ratio > 0.1
        }

    # 5. Skew Analysis
    skew_metrics = {}
    ev_counts = df_combined["event_type"].value_counts()
    skew_metrics["event_type"] = {
        k: {"count": int(v), "pct": round((v / total_rows) * 100, 2)}
        for k, v in ev_counts.items()
    }

    if "category_code" in df_combined.columns:
        cat_counts = df_combined["category_code"].value_counts(dropna=False).head(5)
        skew_metrics["top_categories"] = {
            str(k): {"count": int(v), "pct": round((v / total_rows) * 100, 2)}
            for k, v in cat_counts.items()
        }

    if "brand" in df_combined.columns:
        brand_counts = df_combined["brand"].value_counts(dropna=False).head(5)
        skew_metrics["top_brands"] = {
            str(k): {"count": int(v), "pct": round((v / total_rows) * 100, 2)}
            for k, v in brand_counts.items()
        }

    if "user_id" in df_combined.columns:
        user_counts = df_combined["user_id"].value_counts().head(5)
        skew_metrics["top_users"] = {
            str(k): {"count": int(v), "pct": round((v / total_rows) * 100, 2)}
            for k, v in user_counts.items()
        }

    git_commit = get_git_commit()
    report = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "git_commit": git_commit,
        "mode": manifest.get("mode", "small"),
        "manifest_timestamp": manifest.get("timestamp", "N/A"),
        "source_dataset": "REES46 eCommerce Behavior Data (2019-Oct.csv)",
        "date_ranges": {
            "part1": "2019-10-01 den 2019-10-15 (Schema 9 cot)",
            "part2": "2019-10-16 den 2019-10-25 (Schema 10 cot, co discount_percent)"
        },
        "volume": volume_metrics,
        "schema_evolution": schema_metrics,
        "duplicate_rate": duplicate_metrics,
        "cardinality": cardinality_metrics,
        "skew": skew_metrics
    }
    return report


def generate_markdown_report(profile: dict, output_path: str):
    """Generate structured Markdown report for evidence documentation."""
    v = profile["volume"]
    se = profile["schema_evolution"]
    dup = profile["duplicate_rate"]
    card = profile["cardinality"]
    skew = profile["skew"]

    lines = [
        "# Bao Cao Do Luong Du Lieu Thuc Te (Data Profile Evidence)",
        "",
        f"> **Thoi diem do luong**: `{profile['timestamp']}`  ",
        "> **Lenh da chay**: `make profile-data` (`scripts/profile_generated_data.py`)  ",
        f"> **Git commit**: `{profile['git_commit']}`  ",
        f"> **Che do du lieu (Mode)**: `{profile['mode']}`  ",
        f"> **Tong so dong nguon**: `{v['total_rows']:,}` dong  ",
        f"> **Manifest timestamp**: `{profile['manifest_timestamp']}`  ",
        f"> **Nguon du lieu goc**: {profile['source_dataset']}  ",
        f"> - Part 1: {profile['date_ranges']['part1']}  ",
        f"> - Part 2: {profile['date_ranges']['part2']}  ",
        "",
        "---",
        "",
        "## 1. Quy Mo va Dung Luong (Volume Metric)",
        "",
        "| Phan Doan | So Dong (Rows) | Dung Luong (Bytes) | Dung Luong (MB) | So Cot |",
        "| :--- | :--- | :--- | :--- | :--- |",
        f"| **Part 1 (01/10 - 15/10)** | {v['part1_rows']:,} | {v['part1_bytes']:,} B | {v['part1_mb']:.2f} MB | {se['part1_column_count']} cot |",
        f"| **Part 2 (16/10 - 25/10)** | {v['part2_rows']:,} | {v['part2_bytes']:,} B | {v['part2_mb']:.2f} MB | {se['part2_column_count']} cot |",
        f"| **TONG CONG** | **{v['total_rows']:,}** | **{v['total_bytes']:,} B** | **{v['total_mb']:.2f} MB** | - |",
        "",
        "---",
        "",
        "## 2. Minh Chung Schema Evolution (2d Rubric)",
        "",
        f"- **Schema Part 1 (9 cot nguyen ban)**: `{', '.join(se['part1_columns'])}`",
        f"- **Schema Part 2 (10 cot tien hoa)**: `{', '.join(se['part2_columns'])}`",
        f"- **Cot moi xuat hien tai Part 2**: `{', '.join(se['new_columns_in_part2'])}` (Bat dau tu ngay 16/10).",
        "",
        "---",
        "",
        "## 3. Minh Chung Tiem Loi Duplicate (2d Rubric)",
        "",
        "| Phan Doan | So Dong Duplicate Toan Hang | Ty Le Thuc Te (%) | Muc Tieu Cau Hinh |",
        "| :--- | :--- | :--- | :--- |",
        f"| **Part 1** | {dup['part1_full_duplicates']:,} dong | **{dup['part1_dup_rate_pct']:.2f}%** | ~2.00% |",
        f"| **Part 2** | {dup['part2_full_duplicates']:,} dong | **{dup['part2_dup_rate_pct']:.2f}%** | ~2.00% |",
        f"| **Trung lap theo Khoa Logic** | {dup['total_key_duplicates']:,} dong | **{dup['total_key_dup_rate_pct']:.2f}%** | ~2.00% |",
        "",
        "---",
        "",
        "## 4. Phan Tich Do Lech Khoa (Skewness Analysis)",
        "",
        "### A. Phan phoi hanh vi nguoi dung (event_type - Class Imbalance):",
    ]

    for ev, stat in skew.get("event_type", {}).items():
        lines.append(f"- **`{ev}`**: {stat['count']:,} luot ({stat['pct']:.2f}%)")

    lines.extend([
        "",
        "### B. Top nganh hang (category_code):",
    ])
    for cat, stat in skew.get("top_categories", {}).items():
        lines.append(f"- **`{cat}`**: {stat['count']:,} luot ({stat['pct']:.2f}%)")

    lines.extend([
        "",
        "### C. Top thuong hieu (brand):",
    ])
    for br, stat in skew.get("top_brands", {}).items():
        lines.append(f"- **`{br}`**: {stat['count']:,} luot ({stat['pct']:.2f}%)")

    lines.extend([
        "",
        "### D. Top nguoi dung hoat dong (user_id):",
    ])
    for uid, stat in skew.get("top_users", {}).items():
        lines.append(f"- **`{uid}`**: {stat['count']:,} luot ({stat['pct']:.2f}%)")

    lines.extend([
        "",
        "---",
        "",
        "## 5. Phan Tich Luc Luong Cao (High-Cardinality Analysis)",
        "",
        "| Cot (Column) | So Gia Tri Khong Rong | So Gia Tri Duy Nhat (Unique) | Ty Le Cardinality (Unique/Total) | Phan Loai |",
        "| :--- | :--- | :--- | :--- | :--- |",
    ])

    for col, stat in card.items():
        classification = "High Cardinality" if stat["is_high_cardinality"] else "Low/Medium Cardinality"
        lines.append(
            f"| `{col}` | {stat['non_null_count']:,} | {stat['unique_count']:,} | {stat['cardinality_ratio']:.6f} | {classification} |"
        )

    lines.append("")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info(f"Markdown report generated at: {output_path}")


def main():
    logger.info("Starting Data Profiling...")
    manifest = load_manifest()
    df_p1, df_p2, p1_bytes, p2_bytes = load_data_from_minio_or_local()

    profile = profile_dataset(df_p1, df_p2, p1_bytes, p2_bytes, manifest)

    json_path = "docs/evidence/data_profile.json"
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(profile, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved profile JSON at: {json_path}")

    md_path = "docs/evidence/data_profile.md"
    generate_markdown_report(profile, md_path)

    print("\n" + "=" * 80)
    print("HOAN TAT DO LUONG DU LIEU BATCH")
    print(f"   - Profile JSON:     {json_path}")
    print(f"   - Profile Markdown: {md_path}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
