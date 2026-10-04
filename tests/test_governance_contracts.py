"""Unit tests for Data Governance and Pure Contract Verification functions.

Tests production functions in governance/verify_contracts.py:
  - check_bronze_nulls: passes on 0 nulls, fails when nulls present
  - check_silver_duplicates: passes on unique rows, fails on duplicates
  - check_gold_scd2_integrity: passes on valid intervals and unique current flag, fails on violations
  - check_gold_feast_schema: passes with event_timestamp and created, fails when missing or null
  - check_gold_binary_labels: passes for {0, 1}, fails on outside values
  - emit_assertion_result: verifies DataHub MCP serialization
"""

from unittest.mock import MagicMock
from datahub.metadata.schema_classes import (
    AssertionResultTypeClass,
    AssertionRunEventClass,
)
import pandas as pd

from governance.verify_contracts import (
    check_bronze_nulls,
    check_gold_binary_labels,
    check_gold_feast_schema,
    check_gold_scd2_integrity,
    check_silver_duplicates,
    emit_assertion_result,
)


def test_bronze_null_contract_pure():
    """Bronze contract: user_id must have 0 null values."""
    valid_df = pd.DataFrame({"user_id": [1001, 1002, 1003]})
    res_valid = check_bronze_nulls(valid_df)
    assert res_valid.passed is True
    assert res_valid.observed == 0.0

    invalid_df = pd.DataFrame({"user_id": [1001, None, 1003]})
    res_invalid = check_bronze_nulls(invalid_df)
    assert res_invalid.passed is False
    assert res_invalid.observed == 1.0


def test_silver_dedup_contract_pure():
    """Silver contract: composite key (user_id, event_time, product_id, event_type) must be unique."""
    keys = ["user_id", "event_time", "product_id", "event_type"]
    clean_df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "event_time": [
                "2019-10-01 10:00:00",
                "2019-10-01 10:01:00",
                "2019-10-01 10:02:00",
            ],
            "product_id": [101, 102, 103],
            "event_type": ["view", "cart", "purchase"],
        }
    )
    res_clean = check_silver_duplicates(clean_df, dedup_keys=keys)
    assert res_clean.passed is True
    assert res_clean.observed == 0.0

    dup_df = pd.concat([clean_df, clean_df.iloc[[0]]], ignore_index=True)
    res_dup = check_silver_duplicates(dup_df, dedup_keys=keys)
    assert res_dup.passed is False
    assert res_dup.observed == 1.0


def test_gold_scd2_integrity_contract_pure():
    """Gold SCD2 contract: product_sk not null, valid_from <= valid_to, unique is_current."""
    valid_scd2 = pd.DataFrame(
        {
            "product_sk": [1, 2],
            "product_id": [100, 100],
            "valid_from_ts": [
                pd.Timestamp("2019-10-01"),
                pd.Timestamp("2019-10-15"),
            ],
            "valid_to_ts": [pd.Timestamp("2019-10-14"), pd.NaT],
            "is_current": [False, True],
        }
    )
    res_valid = check_gold_scd2_integrity(valid_scd2)
    assert res_valid.passed is True
    assert res_valid.observed == 0.0

    # Inverted interval violation
    invalid_interval_scd2 = pd.DataFrame(
        {
            "product_sk": [1],
            "product_id": [100],
            "valid_from_ts": [pd.Timestamp("2019-10-20")],
            "valid_to_ts": [pd.Timestamp("2019-10-10")],
            "is_current": [True],
        }
    )
    res_inv = check_gold_scd2_integrity(invalid_interval_scd2)
    assert res_inv.passed is False
    assert res_inv.observed >= 1.0

    # Multiple is_current=True for same product_id violation
    duplicate_current_scd2 = pd.DataFrame(
        {
            "product_sk": [1, 2],
            "product_id": [100, 100],
            "valid_from_ts": [
                pd.Timestamp("2019-10-01"),
                pd.Timestamp("2019-10-15"),
            ],
            "valid_to_ts": [pd.NaT, pd.NaT],
            "is_current": [True, True],
        }
    )
    res_dup_current = check_gold_scd2_integrity(duplicate_current_scd2)
    assert res_dup_current.passed is False
    assert res_dup_current.detail["duplicate_current_products"] == 1


def test_gold_feast_schema_contract_pure():
    """Gold Feast contract: requires event_timestamp and created columns with 0 nulls."""
    valid_features = pd.DataFrame(
        {
            "user_id": [1, 2],
            "event_timestamp": [
                pd.Timestamp("2019-10-25"),
                pd.Timestamp("2019-10-25"),
            ],
            "created": [
                pd.Timestamp("2019-10-25"),
                pd.Timestamp("2019-10-25"),
            ],
            "view_count_30d": [10, 20],
        }
    )
    res_valid = check_gold_feast_schema(valid_features)
    assert res_valid.passed is True
    assert res_valid.observed == 0.0

    # Missing column
    missing_col_df = pd.DataFrame(
        {"user_id": [1], "event_timestamp": [pd.Timestamp("2019-10-25")]}
    )
    res_missing = check_gold_feast_schema(missing_col_df)
    assert res_missing.passed is False

    # Null value in required timestamp
    null_col_df = pd.DataFrame(
        {
            "user_id": [1],
            "event_timestamp": [pd.NaT],
            "created": [pd.Timestamp("2019-10-25")],
        }
    )
    res_null = check_gold_feast_schema(null_col_df)
    assert res_null.passed is False
    assert res_null.observed == 1.0


def test_gold_binary_labels_contract_pure():
    """Gold ML labels contract: target_purchase_1h must be strictly in [0, 1]."""
    valid_labels = pd.DataFrame({"target_purchase_1h": [0, 1, 0, 1, 1, 0]})
    res_valid = check_gold_binary_labels(valid_labels)
    assert res_valid.passed is True
    assert res_valid.observed == 0.0

    # Non-binary labels
    invalid_labels = pd.DataFrame({"target_purchase_1h": [0, 1, 2, -1]})
    res_invalid = check_gold_binary_labels(invalid_labels)
    assert res_invalid.passed is False
    assert res_invalid.observed == 2.0


def test_pure_contracts_missing_columns_edge_cases():
    """Verify that pure contract functions fail gracefully when required columns are absent."""
    empty_df = pd.DataFrame({"dummy": [1, 2, 3]})

    # Bronze missing user_id
    res_bronze = check_bronze_nulls(empty_df)
    assert res_bronze.passed is False
    assert "Missing user_id" in res_bronze.detail.get("error", "")

    # Silver missing keys
    res_silver = check_silver_duplicates(empty_df)
    assert res_silver.passed is False
    assert "missing_keys" in res_silver.detail

    # Gold SCD2 missing product_sk
    res_scd2 = check_gold_scd2_integrity(empty_df)
    assert res_scd2.passed is False
    assert "Missing product_sk" in res_scd2.detail.get("error", "")

    # Gold Feast missing timestamp columns
    res_feast = check_gold_feast_schema(empty_df)
    assert res_feast.passed is False

    # Gold Labels missing label column
    res_labels = check_gold_binary_labels(empty_df)
    assert res_labels.passed is False
    assert "Missing column" in res_labels.detail.get("error", "")


def test_emit_assertion_result_datahub_serialization():
    """Verify emit_assertion_result constructs valid DataHub MetadataChangeProposalWrapper."""
    mock_emitter = MagicMock()
    assertion_urn = "urn:li:assertion:test_assertion"
    dataset_urn = "urn:li:dataset:test_dataset"

    emit_assertion_result(
        emitter=mock_emitter,
        assertion_urn=assertion_urn,
        dataset_urn=dataset_urn,
        is_passed=True,
        metric_name="null_count",
        metric_value=0.0,
        details={"total_rows": 100},
    )

    assert mock_emitter.emit.called
    call_args = mock_emitter.emit.call_args[0][0]
    assert call_args.entityUrn == assertion_urn
    aspect = call_args.aspect
    assert isinstance(aspect, AssertionRunEventClass)
    assert aspect.asserteeUrn == dataset_urn
    assert aspect.result.type == AssertionResultTypeClass.SUCCESS
    assert aspect.result.actualAggValue == 0.0
    assert aspect.result.nativeResults["validation_status"] == "PASSED"
