"""
================================================================================
TEST: BATCH & STREAM DATA GENERATORS (UNIT & PROPERTY-BASED TESTS)
Project: E-Commerce Real-Time Purchase Propensity Prediction System
Author: Hoang Minh Nhan

Validates:
  - Schema evolution between Part 1 (9 canonical cols) and Part 2 (10 canonical cols)
  - Duplicate rate injection across configurable percentages
  - Reproducibility and SeedSequence across (replica, chunk, part)
  - Deterministic User ID offset mapping for replicas
  - Deterministic Product price jitter for SCD2
  - Consistent skew injection for user_id and user_session
  - Pure LateEventBuffer unit tests
  - Hypothesis property-based test for _transform_chunk invariants
================================================================================
"""

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pytest
from hypothesis import given, settings, strategies as st

from src.generator.batch_generator import (
    BatchDataGenerator,
    CANONICAL_9_COLUMNS,
    CANONICAL_10_COLUMNS
)
from src.generator.stream_generator import StreamDataGenerator
from src.generator.late_event_buffer import LateEventBuffer


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
        chunk_idx=0,
        part_idx=0,
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
        chunk_idx=0,
        part_idx=0,
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
        chunk_idx=0,
        part_idx=0,
        duplicate_rate=dup_rate
    )

    expected_dups = int(len(sample_chunk_df) * dup_rate)
    assert n_dup == expected_dups
    assert len(transformed_df) == len(sample_chunk_df) + expected_dups


def test_seed_sequence_reproducibility(batch_generator_instance, sample_chunk_df):
    """Same parameters and seed sequence must yield identical chunks."""
    df1, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=True,
        replica_idx=1,
        chunk_idx=2,
        part_idx=0,
        duplicate_rate=0.05
    )
    df2, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=True,
        replica_idx=1,
        chunk_idx=2,
        part_idx=0,
        duplicate_rate=0.05
    )
    pd.testing.assert_frame_equal(df1, df2)

    # Different chunk_idx must produce different random patterns
    df3, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=True,
        replica_idx=1,
        chunk_idx=3,
        part_idx=0,
        duplicate_rate=0.05
    )
    assert not df1.equals(df3)


def test_deterministic_user_id_offset(batch_generator_instance, sample_chunk_df):
    """
    In replica > 0, every user either retains original ID or maps to user_id + r * offset.
    All rows of the same user must be consistently mapped.
    """
    replica_idx = 2
    offset = batch_generator_instance.id_offset

    transformed_df, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=False,
        replica_idx=replica_idx,
        chunk_idx=0,
        part_idx=0,
        duplicate_rate=0.0
    )

    # Check session suffix
    assert all(transformed_df["user_session"].str.endswith(f"-r{replica_idx}"))

    # Check user consistency: group by original user_id
    sample_chunk_df["new_uid"] = transformed_df["user_id"]
    for orig_uid, group in sample_chunk_df.groupby("user_id"):
        mapped_uids = group["new_uid"].unique()
        assert len(mapped_uids) == 1, f"User {orig_uid} was inconsistently mapped to {mapped_uids}"
        mapped_val = mapped_uids[0]
        assert mapped_val in (orig_uid, orig_uid + replica_idx * offset)


def test_deterministic_price_jitter(batch_generator_instance, sample_chunk_df):
    """
    In replica > 0, price is jittered within +-price_jitter_pct and deterministic per product.
    """
    replica_idx = 1
    jitter_pct = batch_generator_instance.price_jitter_pct

    transformed_df, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=False,
        replica_idx=replica_idx,
        chunk_idx=0,
        part_idx=0,
        duplicate_rate=0.0
    )

    orig_prices = sample_chunk_df["price"].values
    trans_prices = transformed_df["price"].values
    rel_diffs = np.abs(trans_prices - orig_prices) / orig_prices

    assert np.all(rel_diffs <= (jitter_pct / 100.0) + 0.01)


def test_skew_injection_consistent_sessions(batch_generator_instance, sample_chunk_df):
    """
    Skew injection replaces user_id with hot keys AND sets user_session to hot-{key}-{session_idx}.
    """
    batch_generator_instance.skew_cfg = {
        "enabled": True,
        "column": "user_id",
        "top_k_keys": [999999999, 888888888],
        "hot_ratio": 0.40,
        "sessions_per_key": 20
    }

    transformed_df, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(),
        is_part2=False,
        replica_idx=0,
        chunk_idx=0,
        part_idx=0,
        duplicate_rate=0.0
    )

    hot_keys = [999999999, 888888888]
    hot_mask = transformed_df["user_id"].isin(hot_keys)
    assert hot_mask.sum() >= 40

    # Verify session format for hot keys: hot-{key}-{s}
    hot_rows = transformed_df[hot_mask]
    for _, row in hot_rows.iterrows():
        assert row["user_session"].startswith(f"hot-{row['user_id']}-")


def test_late_event_buffer_pure_logic():
    """Unit test for LateEventBuffer methods."""
    buffer = LateEventBuffer()
    assert len(buffer) == 0

    t0 = datetime(2019, 10, 26, 10, 0, 0)
    ev1 = {"user_id": 1, "name": "ev1"}
    ev2 = {"user_id": 2, "name": "ev2"}

    buffer.add(t0 + timedelta(minutes=5), ev1, delay_minutes=5.0)
    buffer.add(t0 + timedelta(minutes=10), ev2, delay_minutes=10.0)
    assert len(buffer) == 2

    # At t0 + 3 min: none ready
    ready = buffer.release_ready(t0 + timedelta(minutes=3))
    assert len(ready) == 0
    assert len(buffer) == 2

    # At t0 + 6 min: ev1 ready
    ready = buffer.release_ready(t0 + timedelta(minutes=6))
    assert ready == [ev1]
    assert len(buffer) == 1

    # Flush all remaining
    flushed = buffer.release_all()
    assert flushed == [ev2]
    assert len(buffer) == 0

    # Delay distribution check
    dist = buffer.get_delay_distribution()
    assert dist["min"] == 5.0
    assert dist["max"] == 10.0
    assert dist["count"] == 2


def test_stream_parse_csv_line():
    """Verify CSV line parsing and schema conversion in StreamDataGenerator."""
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


# Property-based testing with hypothesis
@given(
    dup_rate=st.floats(min_value=0.0, max_value=0.20),
    is_part2=st.booleans()
)
@settings(max_examples=15, deadline=None)
def test_transform_chunk_hypothesis_invariants(dup_rate, is_part2):
    """
    Property:
    1. Output row count == Input row count + n_dup.
    2. Repeated calls with identical parameters yield identical outputs.
    """
    gen = BatchDataGenerator(config_path="config/generator_config.yaml", dry_run=True)
    n_rows = 50
    sample_df = pd.DataFrame({
        "event_time": [f"2019-10-01 10:{i:02d}:00 UTC" for i in range(n_rows)],
        "event_type": ["view"] * n_rows,
        "product_id": list(range(1, n_rows + 1)),
        "category_id": [100] * n_rows,
        "category_code": ["test"] * n_rows,
        "brand": ["brand_a"] * n_rows,
        "price": [50.0] * n_rows,
        "user_id": list(range(100, 100 + n_rows)),
        "user_session": [f"s_{i}" for i in range(n_rows)]
    })

    out1, n_dup1 = gen._transform_chunk(
        sample_df.copy(),
        is_part2=is_part2,
        replica_idx=1,
        chunk_idx=0,
        part_idx=0,
        duplicate_rate=dup_rate
    )
    expected_dups = int(n_rows * dup_rate)
    assert n_dup1 == expected_dups
    assert len(out1) == n_rows + expected_dups

    # Determinism
    out2, n_dup2 = gen._transform_chunk(
        sample_df.copy(),
        is_part2=is_part2,
        replica_idx=1,
        chunk_idx=0,
        part_idx=0,
        duplicate_rate=dup_rate
    )
    pd.testing.assert_frame_equal(out1, out2)
