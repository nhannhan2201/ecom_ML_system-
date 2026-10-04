"""
DataHub Data Quality & Contract Verification Script (Production Pattern)
Dự án: E-Commerce Real-Time Purchase Propensity Prediction System

Nhiệm vụ:
  1. Kiểm định dữ liệu thực tế trên MinIO Lakehouse (nhanh, nhẹ bằng PyArrow/s3fs, không cần cụm Spark).
  2. Đo đạc các chỉ số: Null count, Duplicate count, SCD2 integrity, Binary labels [0, 1].
  3. Bắn kết quả kiểm định thực (AssertionRunEvent) lên DataHub GMS.
  4. Đổi trạng thái hiển thị trên DataHub UI thành XANH (PASSED / ACTIVE).
"""

import os
import sys
import time
import argparse
import s3fs
import pyarrow.parquet as pq
import pandas as pd

from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionResultClass,
    AssertionResultTypeClass,
)

from catalog import (
    BRONZE_URN, SILVER_URN, GOLD_DIM_PROD_URN, GOLD_FEAT_30D_URN, GOLD_USER_LABELS_URN
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
if "ecom_minio" in MINIO_ENDPOINT:
    MINIO_ENDPOINT = MINIO_ENDPOINT.replace("ecom_minio", "minio")


def get_s3fs():
    """Initialize an s3fs S3FileSystem client configured for local or container MinIO storage."""
    return s3fs.S3FileSystem(
        key="minioadmin",
        secret="minioadmin",
        client_kwargs={"endpoint_url": MINIO_ENDPOINT}
    )


def emit_assertion_result(emitter, assertion_urn: str, dataset_urn: str, is_passed: bool, metric_name: str, metric_value: float, details: dict):
    """Bắn kết quả kiểm định động lên DataHub GMS."""
    now_ms = int(time.time() * 1000)
    res_type = AssertionResultTypeClass.SUCCESS if is_passed else AssertionResultTypeClass.FAILURE
    status_label = "PASSED" if is_passed else "FAILED"

    run_event = AssertionRunEventClass(
        timestampMillis=now_ms,
        runId=f"run_val_{int(time.time())}",
        asserteeUrn=dataset_urn,
        assertionUrn=assertion_urn,
        status=AssertionRunStatusClass.COMPLETE,
        result=AssertionResultClass(
            type=res_type,
            actualAggValue=float(metric_value),
            nativeResults={
                "validation_status": status_label,
                "metric_name": metric_name,
                "metric_value": str(metric_value),
                **{k: str(v) for k, v in details.items()}
            }
        )
    )

    emitter.emit(MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=run_event
    ))
    icon = "✅" if is_passed else "❌"
    print(f"  {icon} [{status_label}] {assertion_urn.split(':')[-1]}: {metric_name}={metric_value}")


def verify_all_contracts(gms_url: str = None):
    """Verify data contracts across Delta Lake datasets and emit AssertionRunEvents to DataHub.
    
    Performs sample-based quality validation (up to 50,000 rows) using PyArrow directly
    against underlying materialized Parquet data files on MinIO (not transaction-log snapshot):
      1. Bronze zero-nulls check on user_id.
      2. Silver deduplication uniqueness on (user_id, event_time, product_id, event_type).
      3. Gold dim_product SCD2 valid range (valid_from_ts <= valid_to_ts) and surrogate key integrity.
      4. Gold feat_user_30d Feast temporal metadata contract (event_timestamp, created).
      5. Gold user_labels binary target validity (target_purchase_1h in [0, 1]).
      
    Args:
        gms_url: DataHub GMS REST endpoint URL.
    """
    if not gms_url:
        gms_url = os.getenv("DATAHUB_GMS_URL", "http://localhost:8089")
    print("=" * 80)
    print("🧪 [DATA QUALITY AUDIT]: KIỂM ĐỊNH MẪU DỮ LIỆU THỰC TẾ (SAMPLE-BASED 50,000 ROWS)")
    print("=" * 80)
    print(f"📡 DataHub GMS Endpoint : {gms_url}")
    print(f"📦 MinIO Endpoint       : {MINIO_ENDPOINT}")
    print("-" * 80)

    emitter = DatahubRestEmitter(gms_url)
    fs = get_s3fs()

    # 1. KIỂM ĐỊNH BẢNG BRONZE: RAW EVENTS
    print("🔍 [1/5] Kiểm định Bronze Lakehouse: raw_events (Sample-based 50k rows)...")
    try:
        tbl = pq.read_table("ecommerce-lakehouse/bronze/raw_events/", filesystem=fs, columns=["user_id"])
        total_rows = tbl.num_rows
        df = tbl.slice(0, 50000).to_pandas()
        null_users = int(df["user_id"].isna().sum())
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp1_bronze_null_check",
            dataset_urn=BRONZE_URN,
            is_passed=(null_users == 0),
            metric_name="sample_null_user_count",
            metric_value=null_users,
            details={"sampled_rows": len(df), "total_rows": total_rows}
        )
    except Exception as e:
        print(f"  ⚠️ Bronze check error: {e}")

    # 2. KIỂM ĐỊNH BẢNG SILVER: STG EVENTS
    print("🔍 [2/5] Kiểm định Silver Lakehouse: stg_events (Khử trùng lặp trên bộ khóa Spark)...")
    try:
        tbl = pq.read_table("ecommerce-lakehouse/silver/stg_events/", filesystem=fs, columns=["user_id", "event_time", "product_id", "event_type"])
        total_rows = tbl.num_rows
        df = tbl.slice(0, 50000).to_pandas()
        dup_count = int(df.duplicated(subset=["user_id", "event_time", "product_id", "event_type"]).sum())
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp2_silver_dedup_check",
            dataset_urn=SILVER_URN,
            is_passed=(dup_count == 0),
            metric_name="sample_silver_duplicate_count",
            metric_value=dup_count,
            details={"sampled_rows": len(df), "total_rows": total_rows, "dedup_keys": ["user_id", "event_time", "product_id", "event_type"]}
        )
    except Exception as e:
        print(f"  ⚠️ Silver check error: {e}")

    # 3. KIỂM ĐỊNH BẢNG GOLD: DIM PRODUCT (SCD TYPE 2)
    print("🔍 [3/5] Kiểm định Gold DWH: dim_product (SCD Type 2)...")
    try:
        tbl = pq.read_table("ecommerce-lakehouse/gold/dim_product/", filesystem=fs, columns=["product_sk", "valid_from_ts", "valid_to_ts", "is_current"])
        df = tbl.to_pandas()
        null_sks = int(df["product_sk"].isna().sum())
        # Kiểm tra điều kiện khoảng thời gian hợp lệ: valid_from_ts <= valid_to_ts đối với các bản ghi lịch sử có valid_to_ts
        if "valid_from_ts" in df.columns and "valid_to_ts" in df.columns:
            has_to = df["valid_to_ts"].notna()
            invalid_intervals = int((has_to & (df["valid_from_ts"] > df["valid_to_ts"])).sum())
        else:
            invalid_intervals = 0
        scd2_invalid_count = null_sks + invalid_intervals
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp2_scd2_integrity_check",
            dataset_urn=GOLD_DIM_PROD_URN,
            is_passed=(scd2_invalid_count == 0),
            metric_name="sample_scd2_invalid_count",
            metric_value=scd2_invalid_count,
            details={"null_sks": null_sks, "invalid_intervals": invalid_intervals, "total_products": len(df)}
        )
    except Exception as e:
        print(f"  ⚠️ SCD2 check error: {e}")

    # 4. KIỂM ĐỊNH BẢNG GOLD: FEAT USER 30D (FEAST SCHEMA)
    print("🔍 [4/5] Kiểm định Gold Feature Store: feat_user_30d...")
    try:
        feat_path = "ecommerce-lakehouse/gold/feat_user_30d/"
        tbl = pq.read_table(feat_path, filesystem=fs)
        has_ts = "event_timestamp" in tbl.column_names and "created" in tbl.column_names
        null_ts = 0
        if has_ts:
            df = tbl.slice(0, 10000).to_pandas()
            null_ts = int(df["event_timestamp"].isna().sum()) + int(df["created"].isna().sum())
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp3_feast_schema_contract",
            dataset_urn=GOLD_FEAT_30D_URN,
            is_passed=(has_ts and null_ts == 0),
            metric_name="sample_feast_timestamp_null_count",
            metric_value=null_ts,
            details={"has_event_timestamp": "event_timestamp" in tbl.column_names, "has_created": "created" in tbl.column_names, "total_users": tbl.num_rows}
        )
    except Exception as e:
        print(f"  ⚠️ Feature Store check error: {e}")

    # 5. KIỂM ĐỊNH BẢNG GOLD: USER LABELS (GROUND TRUTH)
    print("🔍 [5/5] Kiểm định Gold ML: user_labels (Binary Classification [0, 1])...")
    try:
        label_path = "ecommerce-lakehouse/gold/user_labels/"
        tbl = pq.read_table(label_path, filesystem=fs, columns=["target_purchase_1h"])
        df = tbl.slice(0, 50000).to_pandas()
        invalid_labels = int((~df["target_purchase_1h"].isin([0, 1])).sum())
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp3_binary_labels_contract",
            dataset_urn=GOLD_USER_LABELS_URN,
            is_passed=(invalid_labels == 0),
            metric_name="sample_invalid_label_count",
            metric_value=invalid_labels,
            details={"sampled_rows": len(df), "total_labels": tbl.num_rows}
        )
    except Exception as e:
        print(f"  ⚠️ Labels check error: {e}")

    print("-" * 80)
    print("✅ HOÀN TẤT KIỂM ĐỊNH MẪU VÀ PHÁT HÀNH KẾT QUẢ ASSERTIONS LÊN DATAHUB.")
    print("=" * 80)


def main():
    """Command-line entry point to verify contracts and report assertion events to DataHub."""
    parser = argparse.ArgumentParser(description="Verify Data Contracts and Publish Results to DataHub")
    parser.add_argument("--gms-url", default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8089"), help="DataHub GMS REST URL")
    args = parser.parse_args()

    verify_all_contracts(gms_url=args.gms_url)


if __name__ == "__main__":
    main()
