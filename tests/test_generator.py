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

import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pytest
from hypothesis import given, settings, strategies as st

from src.generator.batch_generator import BatchDataGenerator, CANONICAL_9_COLUMNS, CANONICAL_10_COLUMNS
from src.generator.stream_generator import StreamDataGenerator
from src.generator.late_event_buffer import LateEventBuffer


@pytest.fixture
def sample_chunk_df():
    """Create a reproducible sample dataframe of 100 e-commerce raw events."""
    n_rows = 100
    times = [f"2019-10-01 {i % 24:02d}:{(i * 2) % 60:02d}:00 UTC" for i in range(n_rows)]
    return pd.DataFrame(
        {
            "event_time": times,
            "event_type": ["view"] * 80 + ["cart"] * 15 + ["purchase"] * 5,
            "product_id": list(range(1001, 1001 + n_rows)),
            "category_id": [205301355] * n_rows,
            "category_code": ["electronics.smartphone"] * n_rows,
            "brand": ["samsung"] * 50 + ["apple"] * 30 + ["xiaomi"] * 20,
            "price": [100.0 + i for i in range(n_rows)],
            "user_id": [50000000 + (i % 20) for i in range(n_rows)],
            "user_session": [f"session_{i % 10}" for i in range(n_rows)],
        }
    )


@pytest.fixture
def batch_generator_instance():
    """Instantiate BatchDataGenerator in dry-run mode to avoid S3 calls."""
    gen = BatchDataGenerator(config_path="config/generator_config.yaml", mode="small", dry_run=True)
    return gen


def test_schema_evolution_part1(batch_generator_instance, sample_chunk_df):
    """Part 1 (01/10 -> 15/10) must contain exactly 9 canonical columns with NO discount_percent."""
    transformed_df, n_dup = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(), is_part2=False, replica_idx=0, chunk_idx=0, part_idx=0, duplicate_rate=0.0
    )

    assert list(transformed_df.columns) == CANONICAL_9_COLUMNS
    assert "discount_percent" not in transformed_df.columns
    assert len(transformed_df) == len(sample_chunk_df)
    assert n_dup == 0


def test_schema_evolution_part2(batch_generator_instance, sample_chunk_df):
    """Part 2 (16/10 -> 25/10) must contain exactly 10 canonical columns including discount_percent."""
    transformed_df, n_dup = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(), is_part2=True, replica_idx=0, chunk_idx=0, part_idx=0, duplicate_rate=0.0
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
        df_chunk=sample_chunk_df.copy(), is_part2=False, replica_idx=0, chunk_idx=0, part_idx=0, duplicate_rate=dup_rate
    )

    expected_dups = int(len(sample_chunk_df) * dup_rate)
    assert n_dup == expected_dups
    assert len(transformed_df) == len(sample_chunk_df) + expected_dups


def test_seed_sequence_reproducibility(batch_generator_instance, sample_chunk_df):
    """Same parameters and seed sequence must yield identical chunks."""
    df1, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(), is_part2=True, replica_idx=1, chunk_idx=2, part_idx=0, duplicate_rate=0.05
    )
    df2, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(), is_part2=True, replica_idx=1, chunk_idx=2, part_idx=0, duplicate_rate=0.05
    )
    pd.testing.assert_frame_equal(df1, df2)

    # Different chunk_idx must produce different random patterns
    df3, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(), is_part2=True, replica_idx=1, chunk_idx=3, part_idx=0, duplicate_rate=0.05
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
        duplicate_rate=0.0,
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
        duplicate_rate=0.0,
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
        "sessions_per_key": 20,
    }

    transformed_df, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(), is_part2=False, replica_idx=0, chunk_idx=0, part_idx=0, duplicate_rate=0.0
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

    header_cols = [
        "event_time",
        "event_type",
        "product_id",
        "category_id",
        "category_code",
        "brand",
        "price",
        "user_id",
        "user_session",
    ]
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
    is_part2=st.booleans(),
    chunk_size=st.integers(min_value=20, max_value=80),
)
@settings(max_examples=15, deadline=None)
def test_transform_chunk_hypothesis_invariants(dup_rate, is_part2, chunk_size):
    """
    Property:
    1. Output row count == Input row count + n_dup, where n_dup = int(chunk_size * dup_rate).
    2. Every injected duplicate row is an exact match of an original row.
    3. Repeated calls with identical parameters yield identical outputs.
    """
    gen = BatchDataGenerator(config_path="config/generator_config.yaml", dry_run=True)
    sample_df = pd.DataFrame(
        {
            "event_time": [f"2019-10-01 10:{(i // 60):02d}:{(i % 60):02d} UTC" for i in range(chunk_size)],
            "event_type": ["view"] * chunk_size,
            "product_id": list(range(1, chunk_size + 1)),
            "category_id": [100] * chunk_size,
            "category_code": ["test"] * chunk_size,
            "brand": ["brand_a"] * chunk_size,
            "price": [50.0] * chunk_size,
            "user_id": list(range(100, 100 + chunk_size)),
            "user_session": [f"s_{i}" for i in range(chunk_size)],
        }
    )

    out1, n_dup1 = gen._transform_chunk(
        sample_df.copy(), is_part2=is_part2, replica_idx=1, chunk_idx=0, part_idx=0, duplicate_rate=dup_rate
    )
    expected_dups = int(chunk_size * dup_rate)
    assert n_dup1 == expected_dups
    assert len(out1) == chunk_size + expected_dups

    # Duplicate identity: exactly n_dup1 rows are exact duplicates of another row
    assert int(out1.duplicated().sum()) == n_dup1

    # Determinism
    out2, n_dup2 = gen._transform_chunk(
        sample_df.copy(), is_part2=is_part2, replica_idx=1, chunk_idx=0, part_idx=0, duplicate_rate=dup_rate
    )
    pd.testing.assert_frame_equal(out1, out2)


def test_batch_generator_dry_run_estimation(batch_generator_instance):
    """Verify run_dry_run_estimation executes without error and prints estimation."""
    batch_generator_instance.run_dry_run_estimation()


def test_batch_generator_modes_initialization():
    """Verify BatchDataGenerator can initialize in small, medium, and full modes."""
    for mode in ["small", "medium", "full"]:
        gen = BatchDataGenerator(config_path="config/generator_config.yaml", mode=mode, dry_run=True)
        assert gen.mode == mode
        assert gen.base_seed == 42


def test_batch_generator_drift_injection(batch_generator_instance, sample_chunk_df):
    """Verify that opt-in drift increases prices by drift_factor on Part 2."""
    batch_generator_instance.drift_cfg = {"enabled": True, "column": "price", "drift_factor": 2.0}
    transformed_df, _ = batch_generator_instance._transform_chunk(
        df_chunk=sample_chunk_df.copy(), is_part2=True, replica_idx=0, chunk_idx=0, part_idx=0, duplicate_rate=0.0
    )
    orig_prices = sample_chunk_df["price"].values
    trans_prices = transformed_df["price"].values
    np.testing.assert_allclose(trans_prices, orig_prices * 2.0, rtol=1e-2)


def test_batch_generator_save_manifest_local(batch_generator_instance, tmp_path, monkeypatch):
    """Verify manifest JSON in a temporary working directory without touching real data."""
    monkeypatch.chdir(tmp_path)
    manifest = {"status": "COMPLETED", "mode": "test", "total_records": 100, "execution_date": "2026-10-04"}
    batch_generator_instance._save_manifest(manifest)
    import json

    with open("data/generation_manifest.json", "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded["status"] == "COMPLETED"
    assert loaded["total_records"] == 100


def test_stream_generator_checkpoint_and_manifest(tmp_path, monkeypatch):
    """Verify checkpoint and manifest operations stay inside the temporary directory."""
    monkeypatch.chdir(tmp_path)
    gen = StreamDataGenerator.__new__(StreamDataGenerator)
    gen.checkpoint_file = str(tmp_path / "stream_checkpoint.json")
    gen.topic_name = "test_topic"
    gen.late_buffer = LateEventBuffer()
    gen.stats = {
        "start_time": time.time() - 10,
        "total_produced": 500,
        "normal_produced": 450,
        "duplicates_injected": 25,
        "late_delayed": 25,
        "late_released": 20,
        "burst_events": 50,
        "last_event_time": "2019-10-26 12:00:00 UTC",
        "byte_offset": 12345,
    }

    # Test save checkpoint
    gen._save_checkpoint(12345)
    import json

    with open(gen.checkpoint_file, "r", encoding="utf-8") as f:
        cp = json.load(f)
    assert cp["byte_offset"] == 12345

    # Test delivery report
    gen._delivery_report(None, None)
    gen._delivery_report("Simulated delivery error", None)

    # Test save stream manifest
    gen.save_stream_manifest(burst_duration=30, burst_multiplier=5)
    with open("data/stream_manifest.json", "r", encoding="utf-8") as f:
        sm = json.load(f)
    assert sm["topic"] == "test_topic"
    assert sm["total_produced"] == 500
    assert sm["burst_config"]["multiplier"] == 5
