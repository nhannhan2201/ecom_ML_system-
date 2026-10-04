"""DataHub Data Quality & Contract Verification Script (Pure Function Pattern)

Kiem dinh chat luong du lieu va hop dong du lieu tren Lakehouse:
  1. Ham thuan kiem tra hop dong (Pure contract functions):
     - check_bronze_nulls: Bronze khong null user_id.
     - check_silver_duplicates: Silver khong trung theo bo khoa logic.
     - check_gold_scd2_integrity: Gold SCD2 product_sk khong null, valid_from <= valid_to, duy nhat is_current.
     - check_gold_feast_schema: Gold Feast co du event_timestamp va created, 0 nulls.
     - check_gold_binary_labels: Gold Labels target_purchase_1h thuoc tap [0, 1].
  2. Emit ket qua assertion len DataHub GMS REST API.
"""

from dataclasses import dataclass
import os
import time
from typing import Any, Dict

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
)
import pandas as pd
import pyarrow.parquet as pq
import s3fs

from catalog import (
    BRONZE_URN,
    GOLD_DIM_PROD_URN,
    GOLD_FEAT_30D_URN,
    GOLD_USER_LABELS_URN,
    SILVER_URN,
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
if "ecom_minio" in MINIO_ENDPOINT:
    MINIO_ENDPOINT = MINIO_ENDPOINT.replace("ecom_minio", "minio")


@dataclass
class ContractResult:
    """Ket qua kiem dinh hop dong du lieu co cau truc."""

    passed: bool
    observed: float
    expected: float
    detail: Dict[str, Any]


def check_bronze_nulls(df: pd.DataFrame) -> ContractResult:
    """Kiem tra hop dong Bronze: user_id khong duoc chua gia tri null."""
    if "user_id" not in df.columns:
        return ContractResult(
            passed=False,
            observed=1.0,
            expected=0.0,
            detail={"error": "Missing user_id column"},
        )
    null_users = int(df["user_id"].isna().sum())
    return ContractResult(
        passed=(null_users == 0),
        observed=float(null_users),
        expected=0.0,
        detail={"null_users": null_users, "total_rows": len(df)},
    )


def check_silver_duplicates(
    df: pd.DataFrame, dedup_keys: list = None
) -> ContractResult:
    """Kiem tra hop dong Silver: bo khoa logic khong duoc phep trung lap."""
    if dedup_keys is None:
        dedup_keys = ["user_id", "event_time", "product_id", "event_type"]
    missing = [k for k in dedup_keys if k not in df.columns]
    if missing:
        return ContractResult(
            passed=False,
            observed=float(len(missing)),
            expected=0.0,
            detail={"missing_keys": missing},
        )
    dup_count = int(df.duplicated(subset=dedup_keys).sum())
    return ContractResult(
        passed=(dup_count == 0),
        observed=float(dup_count),
        expected=0.0,
        detail={
            "dup_count": dup_count,
            "dedup_keys": dedup_keys,
            "total_rows": len(df),
        },
    )


def check_gold_scd2_integrity(df: pd.DataFrame) -> ContractResult:
    """Kiem tra hop dong Gold SCD2:

    - product_sk khong null
    - valid_from_ts <= valid_to_ts
    - moi product_id chi co toi da mot ban ghi is_current = True
    """
    if "product_sk" not in df.columns:
        return ContractResult(
            passed=False,
            observed=1.0,
            expected=0.0,
            detail={"error": "Missing product_sk"},
        )
    null_sks = int(df["product_sk"].isna().sum())

    invalid_intervals = 0
    if "valid_from_ts" in df.columns and "valid_to_ts" in df.columns:
        has_to = df["valid_to_ts"].notna()
        invalid_intervals = int(
            (has_to & (df["valid_from_ts"] > df["valid_to_ts"])).sum()
        )

    duplicate_current = 0
    if "product_id" in df.columns and "is_current" in df.columns:
        current_df = df[df["is_current"].astype(bool)]
        dup_series = current_df.groupby("product_id").size()
        duplicate_current = int((dup_series > 1).sum())

    total_violations = null_sks + invalid_intervals + duplicate_current
    return ContractResult(
        passed=(total_violations == 0),
        observed=float(total_violations),
        expected=0.0,
        detail={
            "null_sks": null_sks,
            "invalid_intervals": invalid_intervals,
            "duplicate_current_products": duplicate_current,
            "total_products": len(df),
        },
    )


def check_gold_feast_schema(df: pd.DataFrame) -> ContractResult:
    """Kiem tra hop dong Feast: bat buoc co event_timestamp va created khong null."""
    has_event_ts = "event_timestamp" in df.columns
    has_created = "created" in df.columns
    if not (has_event_ts and has_created):
        return ContractResult(
            passed=False,
            observed=1.0,
            expected=0.0,
            detail={
                "has_event_timestamp": has_event_ts,
                "has_created": has_created,
            },
        )
    null_event_ts = int(df["event_timestamp"].isna().sum())
    null_created = int(df["created"].isna().sum())
    total_nulls = null_event_ts + null_created
    return ContractResult(
        passed=(total_nulls == 0),
        observed=float(total_nulls),
        expected=0.0,
        detail={
            "null_event_timestamp": null_event_ts,
            "null_created": null_created,
            "total_rows": len(df),
        },
    )


def check_gold_binary_labels(
    df: pd.DataFrame, label_col: str = "target_purchase_1h"
) -> ContractResult:
    """Kiem tra hop dong nhan: target_purchase_1h phai thuoc tap {0, 1} va khong null."""
    if label_col not in df.columns:
        return ContractResult(
            passed=False,
            observed=1.0,
            expected=0.0,
            detail={"error": f"Missing column {label_col}"},
        )
    null_count = int(df[label_col].isna().sum())
    valid_binary = df[label_col].dropna().isin([0, 1])
    invalid_values = int((~valid_binary).sum())
    total_invalid = null_count + invalid_values
    return ContractResult(
        passed=(total_invalid == 0),
        observed=float(total_invalid),
        expected=0.0,
        detail={
            "null_labels": null_count,
            "non_binary_labels": invalid_values,
            "total_rows": len(df),
        },
    )


def get_s3fs():
    """Khoi tao s3fs S3FileSystem ket noi MinIO."""
    access_key = (
        os.getenv("MINIO_ACCESS_KEY")
        or os.getenv("AWS_ACCESS_KEY_ID")
        or "minioadmin"
    )
    secret_key = (
        os.getenv("MINIO_SECRET_KEY")
        or os.getenv("AWS_SECRET_ACCESS_KEY")
        or "minioadmin"
    )

    return s3fs.S3FileSystem(
        key=access_key,
        secret=secret_key,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def emit_assertion_result(
    emitter,
    assertion_urn: str,
    dataset_urn: str,
    is_passed: bool,
    metric_name: str,
    metric_value: float,
    details: dict,
):
    """Ban ket qua kiem dinh dong len DataHub GMS."""
    now_ms = int(time.time() * 1000)
    res_type = (
        AssertionResultTypeClass.SUCCESS
        if is_passed
        else AssertionResultTypeClass.FAILURE
    )
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
                **{k: str(v) for k, v in details.items()},
            },
        ),
    )

    emitter.emit(
        MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=run_event)
    )
    print(
        f"  [{status_label}] {assertion_urn.split(':')[-1]}: {metric_name}={metric_value}"
    )


def verify_all_contracts(gms_url: str = None):
    """Doc du lieu Lakehouse, goi ham thuan kiem dinh va emit len DataHub."""
    if not gms_url:
        gms_url = os.getenv("DATAHUB_GMS_URL", "http://localhost:8089")
    print("=" * 80)
    print(
        "[DATA QUALITY AUDIT] Kiem dinh hop dong du lieu Lakehouse (Sample-based 50k rows)"
    )
    print("=" * 80)
    print(f"DataHub GMS Endpoint : {gms_url}")
    print(f"MinIO Endpoint       : {MINIO_ENDPOINT}")
    print("-" * 80)

    emitter = DatahubRestEmitter(gms_url)
    fs = get_s3fs()

    # 1. BRONZE
    print("[1/5] Kiem dinh Bronze Lakehouse: raw_events...")
    try:
        tbl = pq.read_table(
            "ecommerce-lakehouse/bronze/raw_events/",
            filesystem=fs,
            columns=["user_id"],
        )
        df = tbl.slice(0, 50000).to_pandas()
        res = check_bronze_nulls(df)
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp1_bronze_null_check",
            dataset_urn=BRONZE_URN,
            is_passed=res.passed,
            metric_name="sample_null_user_count",
            metric_value=res.observed,
            details=res.detail,
        )
    except Exception as e:
        print(f"  [ERROR] Bronze check error: {e}")

    # 2. SILVER
    print("[2/5] Kiem dinh Silver Lakehouse: stg_events...")
    try:
        tbl = pq.read_table(
            "ecommerce-lakehouse/silver/stg_events/",
            filesystem=fs,
            columns=["user_id", "event_time", "product_id", "event_type"],
        )
        df = tbl.slice(0, 50000).to_pandas()
        res = check_silver_duplicates(df)
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp2_silver_dedup_check",
            dataset_urn=SILVER_URN,
            is_passed=res.passed,
            metric_name="sample_silver_duplicate_count",
            metric_value=res.observed,
            details=res.detail,
        )
    except Exception as e:
        print(f"  [ERROR] Silver check error: {e}")

    # 3. GOLD SCD2
    print("[3/5] Kiem dinh Gold DWH: dim_product (SCD Type 2)...")
    try:
        tbl = pq.read_table(
            "ecommerce-lakehouse/gold/dim_product/",
            filesystem=fs,
            columns=[
                "product_sk",
                "product_id",
                "valid_from_ts",
                "valid_to_ts",
                "is_current",
            ],
        )
        df = tbl.to_pandas()
        res = check_gold_scd2_integrity(df)
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp2_scd2_integrity_check",
            dataset_urn=GOLD_DIM_PROD_URN,
            is_passed=res.passed,
            metric_name="sample_scd2_invalid_count",
            metric_value=res.observed,
            details=res.detail,
        )
    except Exception as e:
        print(f"  [ERROR] SCD2 check error: {e}")

    # 4. GOLD FEAST SCHEMA
    print("[4/5] Kiem dinh Gold Feature Store: feat_user_30d...")
    try:
        tbl = pq.read_table(
            "ecommerce-lakehouse/gold/feat_user_30d/", filesystem=fs
        )
        df = tbl.slice(0, 10000).to_pandas()
        res = check_gold_feast_schema(df)
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp3_feast_schema_contract",
            dataset_urn=GOLD_FEAT_30D_URN,
            is_passed=res.passed,
            metric_name="sample_feast_timestamp_null_count",
            metric_value=res.observed,
            details=res.detail,
        )
    except Exception as e:
        print(f"  [ERROR] Feature Store check error: {e}")

    # 5. GOLD USER LABELS
    print("[5/5] Kiem dinh Gold ML: user_labels (Binary [0, 1])...")
    try:
        tbl = pq.read_table(
            "ecommerce-lakehouse/gold/user_labels/",
            filesystem=fs,
            columns=["target_purchase_1h"],
        )
        df = tbl.slice(0, 50000).to_pandas()
        res = check_gold_binary_labels(df)
        emit_assertion_result(
            emitter=emitter,
            assertion_urn="urn:li:assertion:dp3_binary_labels_contract",
            dataset_urn=GOLD_USER_LABELS_URN,
            is_passed=res.passed,
            metric_name="sample_invalid_label_count",
            metric_value=res.observed,
            details=res.detail,
        )
    except Exception as e:
        print(f"  [ERROR] Labels check error: {e}")

    print("-" * 80)
    print("Hoan tat kiem dinh mau va phat hanh ket qua len DataHub.")
    print("=" * 80)


def main():
    """Command-line entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Verify Data Contracts and Publish Results to DataHub"
    )
    parser.add_argument(
        "--gms-url",
        default=os.getenv("DATAHUB_GMS_URL", "http://localhost:8089"),
        help="DataHub GMS REST URL",
    )
    args = parser.parse_args()

    verify_all_contracts(gms_url=args.gms_url)


if __name__ == "__main__":
    main()
