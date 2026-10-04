"""
Unit tests for Data Governance and Contract Verification logic.
Validates:
  - Bronze zero-nulls contract on user_id
  - Silver deduplication contract on (user_id, event_time, product_id, event_type)
  - Gold dim_product SCD Type 2 interval and surrogate key integrity
  - Gold feat_user_30d Feast temporal contract (event_timestamp, created)
  - Gold user_labels binary label domain contract ([0, 1])
  - DataHub AssertionRunEvent serialization and status emission
"""

from unittest.mock import MagicMock
import pandas as pd

from governance.verify_contracts import emit_assertion_result
from datahub.metadata.schema_classes import (
    AssertionRunEventClass,
    AssertionResultTypeClass,
)


def test_bronze_null_contract_logic():
    """Bronze contract: user_id must have 0 null values."""
    valid_df = pd.DataFrame({"user_id": [1001, 1002, 1003]})
    assert int(valid_df["user_id"].isna().sum()) == 0

    invalid_df = pd.DataFrame({"user_id": [1001, None, 1003]})
    assert int(invalid_df["user_id"].isna().sum()) == 1


def test_silver_dedup_contract_logic():
    """Silver contract: composite key (user_id, event_time, product_id, event_type) must be unique."""
    keys = ["user_id", "event_time", "product_id", "event_type"]
    clean_df = pd.DataFrame({
        "user_id": [1, 2, 3],
        "event_time": ["2019-10-01 10:00:00", "2019-10-01 10:01:00", "2019-10-01 10:02:00"],
        "product_id": [101, 102, 103],
        "event_type": ["view", "cart", "purchase"]
    })
    dup_count = int(clean_df.duplicated(subset=keys).sum())
    assert dup_count == 0

    dup_df = pd.concat([clean_df, clean_df.iloc[[0]]], ignore_index=True)
    dup_count_invalid = int(dup_df.duplicated(subset=keys).sum())
    assert dup_count_invalid == 1


def test_gold_scd2_integrity_contract_logic():
    """Gold SCD2 contract: product_sk must not be null and valid_from_ts <= valid_to_ts."""
    valid_scd2 = pd.DataFrame({
        "product_sk": [1, 2],
        "valid_from_ts": [pd.Timestamp("2019-10-01"), pd.Timestamp("2019-10-15")],
        "valid_to_ts": [pd.Timestamp("2019-10-14"), pd.NaT],
        "is_current": [False, True]
    })
    null_sks = int(valid_scd2["product_sk"].isna().sum())
    has_to = valid_scd2["valid_to_ts"].notna()
    invalid_intervals = int((has_to & (valid_scd2["valid_from_ts"] > valid_scd2["valid_to_ts"])).sum())
    assert null_sks + invalid_intervals == 0

    # Inverted timestamps
    invalid_scd2 = pd.DataFrame({
        "product_sk": [1],
        "valid_from_ts": [pd.Timestamp("2019-10-20")],
        "valid_to_ts": [pd.Timestamp("2019-10-10")],
        "is_current": [False]
    })
    has_to_inv = invalid_scd2["valid_to_ts"].notna()
    invalid_count = int((has_to_inv & (invalid_scd2["valid_from_ts"] > invalid_scd2["valid_to_ts"])).sum())
    assert invalid_count == 1


def test_gold_feast_schema_contract_logic():
    """Gold Feast contract: requires event_timestamp and created columns with 0 nulls."""
    valid_features = pd.DataFrame({
        "user_id": [1, 2],
        "event_timestamp": [pd.Timestamp("2019-10-25"), pd.Timestamp("2019-10-25")],
        "created": [pd.Timestamp("2019-10-25"), pd.Timestamp("2019-10-25")],
        "view_count_30d": [10, 20]
    })
    has_ts = "event_timestamp" in valid_features.columns and "created" in valid_features.columns
    null_ts = int(valid_features["event_timestamp"].isna().sum()) + int(valid_features["created"].isna().sum())
    assert has_ts and null_ts == 0

    # Missing created column
    missing_col_df = pd.DataFrame({
        "user_id": [1],
        "event_timestamp": [pd.Timestamp("2019-10-25")]
    })
    assert not ("event_timestamp" in missing_col_df.columns and "created" in missing_col_df.columns)


def test_gold_binary_labels_contract_logic():
    """Gold ML labels contract: target_purchase_1h must be strictly in [0, 1]."""
    valid_labels = pd.DataFrame({"target_purchase_1h": [0, 1, 0, 1, 1, 0]})
    invalid_count = int((~valid_labels["target_purchase_1h"].isin([0, 1])).sum())
    assert invalid_count == 0

    invalid_labels = pd.DataFrame({"target_purchase_1h": [0, 1, 2, -1]})
    invalid_count_bad = int((~invalid_labels["target_purchase_1h"].isin([0, 1])).sum())
    assert invalid_count_bad == 2


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
        details={"total_rows": 100}
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
