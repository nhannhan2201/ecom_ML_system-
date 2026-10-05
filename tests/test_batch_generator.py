"""Batch Generator only: classification and sampling, plus isolated transformation handoff."""
from pathlib import Path
import json

import pandas as pd
import pytest

from src.generator.batch_generator import BatchDataGenerator, CANONICAL_9_COLUMNS, CANONICAL_10_COLUMNS

FIXTURE = Path(__file__).parent / "fixtures/batch_generator_october_boundaries.csv.fixture"
EXPECTED = ["NEW", "EXCLUDED", "OLD", "OLD", "EXCLUDED", "NEW", "OLD", "INVALID",
            "NEW", "NEW", "OLD", "NEW"]


def generator(sample_size=1000):
    gen = BatchDataGenerator(dry_run=True, sample_size=sample_size)
    gen.input_csv = str(FIXTURE)
    gen.chunk_size = 3
    return gen


def test_every_boundary_and_partition():
    gen = generator()
    frame = pd.read_csv(FIXTURE)
    actual = gen._classify_chunk(frame)
    print("\nRow ID | event_time | expected | actual | pass")
    for row, expected, classification in zip(frame.itertuples(), EXPECTED, actual):
        print(f"{row.user_id} | {row.event_time} | {expected} | {classification} | {expected == classification}")
    assert actual.tolist() == EXPECTED
    old = set(frame.loc[actual.eq("OLD"), "user_id"])
    new = set(frame.loc[actual.eq("NEW"), "user_id"])
    assert old.isdisjoint(new)
    assert old | new == {101, 103, 104, 106, 107, 109, 110, 111, 112}
    assert not (old | new) & {102, 105, 108}


def test_source_order_independent_membership():
    gen = generator()
    frame = pd.read_csv(FIXTURE)
    reversed_frame = frame.iloc[::-1].reset_index(drop=True)
    first = dict(zip(frame.user_id, gen._classify_chunk(frame)))
    second = dict(zip(reversed_frame.user_id, gen._classify_chunk(reversed_frame)))
    assert first == second


def test_deterministic_sampling_and_shortfall():
    first, second = generator(6), generator(6)
    selected_first, selected_second = first._sample_classified_rows(), second._sample_classified_rows()
    for group, left, right in zip(("OLD", "NEW"), selected_first, selected_second):
        pd.testing.assert_frame_equal(left, right)
        assert len(left) == 3
        assert left.user_id.is_unique
        assert first._classify_chunk(left).eq(group).all()
    assert first.selection_counts == {"OLD": 4, "NEW": 5, "EXCLUDED": 2, "INVALID": 1}
    shortfall = generator(1000)
    old, new = shortfall._sample_classified_rows()
    assert (len(old), len(new)) == (4, 5)
    assert old.user_id.is_unique and new.user_id.is_unique
    print("Counts:", shortfall.selection_counts, "Selected:", shortfall.selected_source_counts)


def test_chunk_size_does_not_change_sample():
    first, second = generator(5), generator(5)
    second.chunk_size = 8
    for left, right in zip(first._sample_classified_rows(), second._sample_classified_rows()):
        assert set(left.user_id) == set(right.user_id)
    assert first.selected_source_counts == {"OLD": 2, "NEW": 3}


def test_local_output_readback_and_empty_group(tmp_path):
    gen = generator()
    gen.local_output_dir = str(tmp_path / "output")
    gen.stats_only = True
    gen.run_sample_mode()
    old = pd.read_csv(tmp_path / "output/raw_events_old.csv")
    new = pd.read_csv(tmp_path / "output/raw_events_new.csv")
    assert list(old.columns) == CANONICAL_9_COLUMNS
    assert list(new.columns) == CANONICAL_10_COLUMNS
    assert gen._classify_chunk(old).eq("OLD").all()
    assert gen._classify_chunk(new).eq("NEW").all()
    manifest = json.loads((tmp_path / "output/generation_manifest.json").read_text())
    assert manifest["selected_source_counts"] == {"OLD": 4, "NEW": 5}
    only_old = pd.read_csv(FIXTURE).iloc[[2]]
    source = tmp_path / "old_only.csv"
    only_old.to_csv(source, index=False)
    gen.input_csv = str(source)
    gen.run_sample_mode()
    assert pd.read_csv(tmp_path / "output/raw_events_new.csv").empty


@pytest.mark.parametrize("sample_size", [0, -1])
def test_invalid_sample_size(sample_size):
    with pytest.raises(ValueError, match="positive integer"):
        generator(sample_size)


def test_invalid_boundary_contract():
    gen = generator()
    gen.batch_cfg["date_range"]["start_timestamp"] = "2019-11-01T00:00:00Z"
    with pytest.raises(ValueError, match="start < effective < end"):
        gen._load_batch_boundaries()


def test_malformed_timestamp_is_not_accepted():
    frame = pd.DataFrame({"event_time": [None, "2019-10-32 00:00:00 UTC",
                                        "2019-10-16 00:00:00", "2019-10-16 25:00:00 UTC"]})
    assert generator()._classify_chunk(frame).eq("INVALID").all()
