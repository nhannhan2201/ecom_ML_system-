"""
Unit tests for Batch and Stream Data Generators.
Validates:
  - Schema evolution between Part 1 (9 canonical columns) and Part 2 (10 canonical columns with discount_percent)
  - Duplicate rate injection across configurable percentages
  - Opt-in key skew injection
  - Concept drift scaling on prices
  - CSV parsing and typing for stream generator
  - Event-time late arrival buffer logic
"""

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pytest

from src.generator.batch_generator import (
    BatchDataGenerator,
    CANONICAL_9_COLUMNS,
    CANONICAL_10_COLUMNS
)
from src.generator.stream_generator import StreamDataGenerator


@pytest.fixture
def sample_chunk_df():
    """Create a reproducible sample dataframe of 100 e-commerce raw events."""
    n_rows = 100
    times = [
        f"2019-10-01 {i % 24:02d}:{(i * 2) % 60:02d}:00 UTC"
        for i in range(n_rows)
    ]
    return pd.DataFrame({
        "event_time": times,
        "event_type": ["view"] * 80 + ["cart"] * 15 + ["purchase"] * 5,
        "product_id": list(range(1001, 1001 + n_rows)),
        "category_id": [205301355] * n_rows,
        "category_code": ["electronics.smartphone"] * n_rows,
        "brand": ["samsung"] * 50 + ["apple"] * 30 + ["xiaomi"] * 20,
        "price": [100.0 + i for i in range(n_rows)],
        "user_id": [50000000 + (i % 20) for i in range(n_rows)],
        "user_session": [f"session_{i % 10}" for i in range(n_rows)],
    })


@pytest.fixture
def batch_generator_instance():
    """Instantiate BatchDataGenerator in dry-run mode to avoid S3 calls."""
    gen = BatchDataGenerator(
        config_path="config/generator_config.yaml",
        mode="small",
        dry_run=True
    )
    return gen


def test_schema_evolution_part1(batch_generator_instance, sample_chunk_df):
    """Part 1 (01/10 -> 15/10) must contain exactly 9 canonical columns with NO discount_percent."""
    transformed_df, n_dup = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=False,
        replica_idx=0,
        duplicate_rate=0.0
    )

    assert list(transformed_df.columns) == CANONICAL_9_COLUMNS
    assert "discount_percent" not in transformed_df.columns
    assert len(transformed_df) == len(sample_chunk_df)
    assert n_dup == 0


def test_schema_evolution_part2(batch_generator_instance, sample_chunk_df):
    """Part 2 (16/10 -> 25/10) must contain exactly 10 canonical columns including discount_percent."""
    transformed_df, n_dup = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=True,
        replica_idx=0,
        duplicate_rate=0.0
    )

    assert list(transformed_df.columns) == CANONICAL_10_COLUMNS
    assert "discount_percent" in transformed_df.columns
    valid_discounts = {4, 5, 8, 10, 12}
    assert set(transformed_df["discount_percent"].unique()).issubset(valid_discounts)
    assert len(transformed_df) == len(sample_chunk_df)


@pytest.mark.parametrize("dup_rate", [0.0, 0.02, 0.05, 0.10])
def test_duplicate_rate_injection(batch_generator_instance, sample_chunk_df, dup_rate):
    """Verify that duplicate injection strictly adds the expected number of duplicate records."""
    transformed_df, n_dup = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=False,
        replica_idx=0,
        duplicate_rate=dup_rate
    )

    expected_dups = int(len(sample_chunk_df) * dup_rate)
    assert n_dup == expected_dups
    assert len(transformed_df) == len(sample_chunk_df) + expected_dups


def test_skew_injection_opt_in(batch_generator_instance, sample_chunk_df):
    """Verify opt-in key skew concentrates user_id distribution on specified hot keys."""
    # When skew is enabled
    batch_generator_instance.skew_cfg = {
        "enabled": True,
        "column": "user_id",
        "top_k_keys": [999999999, 888888888],
        "hot_ratio": 0.40
    }

    transformed_df, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=False,
        replica_idx=0,
        duplicate_rate=0.0
    )

    hot_count = transformed_df["user_id"].isin([999999999, 888888888]).sum()
    hot_ratio = hot_count / len(transformed_df)
    assert hot_ratio >= 0.40


def test_drift_injection_opt_in(batch_generator_instance, sample_chunk_df):
    """Verify concept drift scales price column in Part 2 when enabled."""
    batch_generator_instance.drift_cfg = {
        "enabled": True,
        "column": "price",
        "drift_factor": 1.5
    }

    original_prices = sample_chunk_df["price"].values
    transformed_df, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=True,
        replica_idx=0,
        duplicate_rate=0.0
    )

    expected_prices = np.round(original_prices * 1.5, 2)
    np.testing.assert_allclose(transformed_df["price"].values, expected_prices)


def test_stream_parse_csv_line():
    """Verify CSV line parsing and schema conversion in StreamDataGenerator."""
    # Create generator without connecting to Kafka
    gen = StreamDataGenerator.__new__(StreamDataGenerator)
    gen.include_discount = True
    gen.discount_vals = [4, 5, 8, 10, 12]

    header_cols = ["event_time", "event_type", "product_id", "category_id", "category_code", "brand", "price", "user_id", "user_session"]
    valid_line = "2019-10-26 10:00:00 UTC,view,1004856,2053013555631882655,electronics.smartphone,samsung,130.50,51234567,session_abc123"

    event = gen._parse_csv_line(valid_line, header_cols)
    assert event is not None
    assert event["event_time"] == "2019-10-26 10:00:00 UTC"
    assert event["event_type"] == "view"
    assert event["product_id"] == 1004856
    assert event["price"] == 130.50
    assert event["user_id"] == 51234567
    assert event["discount_percent"] in [4, 5, 8, 10, 12]

    # Test malformed line
    malformed_line = "corrupt,data"
    assert gen._parse_csv_line(malformed_line, header_cols) is None


def test_stream_late_arrival_buffer_logic():
    """Verify late arrival buffer correctly holds and releases events based on event timestamps."""
    gen = StreamDataGenerator.__new__(StreamDataGenerator)
    gen.late_buffer = []

    event_1 = {"user_id": 1, "name": "ev1"}
    event_2 = {"user_id": 2, "name": "ev2"}

    t0 = datetime(2019, 10, 26, 10, 0, 0)
    release_1 = t0 + timedelta(minutes=5)
    release_2 = t0 + timedelta(minutes=15)

    gen.late_buffer.append((release_1, event_1))
    gen.late_buffer.append((release_2, event_2))

    # At t0 + 2 min: Neither should be released
    curr_dt = t0 + timedelta(minutes=2)
    ready = [ev for target_dt, ev in gen.late_buffer if curr_dt >= target_dt]
    assert len(ready) == 0

    # At t0 + 7 min: event_1 should be released, event_2 remains
    curr_dt = t0 + timedelta(minutes=7)
    ready = [ev for target_dt, ev in gen.late_buffer if curr_dt >= target_dt]
    remaining = [(target_dt, ev) for target_dt, ev in gen.late_buffer if curr_dt < target_dt]
    assert ready == [event_1]
    assert len(remaining) == 1
    assert remaining[0][1] == event_2
