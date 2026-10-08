"""Batch Generator only: classification and sampling, plus isolated transformation handoff."""
from pathlib import Path
import json
import csv
import io
import yaml

import pandas as pd
import pytest

from src.generator.batch_generator import BatchDataGenerator, CANONICAL_9_COLUMNS, CANONICAL_10_COLUMNS, ingest_source, verify_source, _MultipartCSV

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


class MemoryMinIO:
    """In-memory S3 transport; never connects to an external service."""

    def __init__(self):
        self.objects, self.uploads = {}, {}
        self.aborted = 0
        self.fail_part = False

    def head_bucket(self, **kwargs):
        return {}

    def list_objects_v2(self, Prefix, **kwargs):
        return {"KeyCount": sum(key.startswith(Prefix) for key in self.objects)}

    def list_multipart_uploads(self, **kwargs):
        return {}

    def create_multipart_upload(self, Key, **kwargs):
        self.uploads[Key] = []
        return {"UploadId": Key}

    def upload_part(self, Key, Body, **kwargs):
        if self.fail_part:
            raise RuntimeError("injected upload failure")
        self.uploads[Key].append(Body)
        return {"ETag": str(len(self.uploads[Key]))}

    def complete_multipart_upload(self, Key, **kwargs):
        self.objects[Key] = b"".join(self.uploads.pop(Key))

    def abort_multipart_upload(self, Key, **kwargs):
        self.uploads.pop(Key, None)
        self.aborted += 1

    def put_object(self, Key, Body, **kwargs):
        self.objects[Key] = Body

    def get_object(self, Key, **kwargs):
        class Body(io.BytesIO):
            def iter_chunks(self, chunk_size):
                while block := self.read(chunk_size):
                    yield block
        return {"Body": Body(self.objects[Key])}


def source_fixture(tmp_path, rows):
    source = tmp_path / "input.csv"
    with source.open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(CANONICAL_9_COLUMNS)
        writer.writerows(rows)
    return source


def test_boundary_and_conservation(tmp_path):
    config = yaml.safe_load(Path("config/generator_config.yaml").read_text())
    rows = [
        ["2019-10-16 00:00:00 UTC", "purchase", "1", "2", "", "", "12.30", "3", ""],
        ["2019-10-15 23:59:59 UTC", "view", "1", "2", "", "", "12.30", "3", ""],
    ]
    rows.append(rows[0].copy())
    source = source_fixture(tmp_path, rows)
    before = source.read_bytes()
    output, client, prefix = tmp_path / "run", MemoryMinIO(), "source/rees46/2019-10/test"
    ingest_source(source, output, config, client, prefix)
    old = list(csv.reader(io.StringIO(client.objects[prefix + "/raw_events_old.csv"].decode())))
    new = list(csv.reader(io.StringIO(client.objects[prefix + "/raw_events_new.csv"].decode())))
    assert old == [CANONICAL_9_COLUMNS, rows[1]]
    assert new[0] == CANONICAL_10_COLUMNS
    assert [row[:9] for row in new[1:]] == [rows[0], rows[2]]
    assert all(int(row[9]) in [4, 5, 8, 10, 12] for row in new[1:])
    assert source.read_bytes() == before
    assert {p.name for p in output.iterdir()} == {"manifest.json"}
    assert json.loads((output / "manifest.json").read_text())["total_records"] == 3
    verify_source(output, config, client, prefix)
    assert json.loads((output / "readback.json").read_text())["status"] == "READBACK_PASS"
    with pytest.raises(ValueError, match="nonempty"):
        ingest_source(source, tmp_path / "another", config, client, prefix)
    client.objects[prefix + "/raw_events_old.csv"] += b"corrupt"
    with pytest.raises(ValueError, match="mismatch"):
        verify_source(output, config, client, prefix)


@pytest.mark.parametrize("timestamp", ["bad", "2019-11-01 00:00:00 UTC"])
def test_invalid_source_aborts_without_complete_manifest(tmp_path, timestamp):
    config = yaml.safe_load(Path("config/generator_config.yaml").read_text())
    source = source_fixture(tmp_path, [[timestamp, "view", "1", "2", "", "", "1", "3", ""]])
    client = MemoryMinIO()
    with pytest.raises(ValueError):
        ingest_source(source, tmp_path / "run", config, client, "source/rees46/2019-10/test")
    assert not (tmp_path / "run/manifest.json").exists()
    assert not client.objects and not client.uploads
    assert client.aborted == 2


def test_upload_failure_aborts_unfinished_objects(tmp_path):
    config = yaml.safe_load(Path("config/generator_config.yaml").read_text())
    source = source_fixture(tmp_path, [])
    client = MemoryMinIO()
    client.fail_part = True
    with pytest.raises(RuntimeError, match="injected"):
        ingest_source(source, tmp_path / "run", config, client, "source/rees46/2019-10/test")
    assert not client.uploads and not client.objects
    assert not (tmp_path / "run/manifest.json").exists()


def test_multipart_pieces_form_one_object():
    client = MemoryMinIO()
    sink = _MultipartCSV(client, "bucket", "one.csv", 5)
    sink.write("hello")
    sink.write("world")
    sink.write("!")
    result = sink.finish()
    assert client.objects == {"one.csv": b"helloworld!"}
    assert result["bytes"] == 11


def test_source_duplicate_quota_and_exact_copies(tmp_path):
    config = yaml.safe_load(Path("config/generator_config.yaml").read_text())
    rows = [[date, "view", str(i), "2", "", "", "1", "3", ""]
            for date in ["2019-10-15 23:59:59 UTC", "2019-10-16 00:00:00 UTC"]
            for i in range(101)]
    source = source_fixture(tmp_path, rows)
    client, prefix = MemoryMinIO(), "source/rees46/2019-10/duplicate"
    ingest_source(source, tmp_path / "run", config, client, prefix)
    manifest = json.loads((tmp_path / "run/manifest.json").read_text())
    assert manifest["timing"]["elapsed_seconds"] >= 0
    assert manifest["timing"]["started_timestamp"] <= manifest["timing"]["finished_timestamp"]
    assert manifest["counts"] == {"old": 101, "new": 101}
    assert manifest["duplicate_counts"] == {"old": 2, "new": 2}
    assert manifest["output_counts"] == {"old": 103, "new": 103}
    evidence_path = tmp_path / "evidence.json"
    verify_source(tmp_path / "run", config, client, prefix, evidence_path)
    evidence = json.loads(evidence_path.read_text())
    assert evidence["status"] == "VERIFIED_DECLARED_CHECKS"
    assert evidence["manifest"]["injected_duplicates"] == 4
    assert evidence["readback"]["audits"]["old"]["injected_duplicate_rate_per_source"] == 2 / 101
    assert len(evidence["runtime"]["code"]["sha256"]) == 64
    audit = json.loads((tmp_path / "run/readback.json").read_text())["audits"]
    assert audit["new"]["injected_copy_pairs"] == 2
    assert json.loads((tmp_path / "run/readback.json").read_text())["timing"]["elapsed_seconds"] >= 0
    for group in ("old", "new"):
        output = list(csv.reader(io.StringIO(client.objects[prefix + "/raw_events_" + group + ".csv"].decode())))[1:]
        assert output[49] == output[50]
        assert output[100] == output[101]
        originals = [row[:9] for i, row in enumerate(output) if i not in (50, 101)]
        assert originals == rows[:101] if group == "old" else originals == rows[101:]


def test_source_duplicate_disabled(tmp_path):
    config = yaml.safe_load(Path("config/generator_config.yaml").read_text())
    config["batch_generator"]["fault_injection"]["duplicate"]["enabled"] = False
    source = source_fixture(tmp_path, [["2019-10-01 00:00:00 UTC", "view", "1", "2", "", "", "1", "3", ""]] * 100)
    client = MemoryMinIO()
    ingest_source(source, tmp_path / "run", config, client, "source/rees46/2019-10/test")
    assert json.loads((tmp_path / "run/manifest.json").read_text())["injected_duplicates"] == 0


def test_failed_verify_does_not_publish_evidence(tmp_path):
    config = yaml.safe_load(Path("config/generator_config.yaml").read_text())
    source = source_fixture(tmp_path, [["2019-10-01 00:00:00 UTC", "view", "1", "2", "", "", "1", "3", ""]])
    client, prefix = MemoryMinIO(), "source/rees46/2019-10/test"
    ingest_source(source, tmp_path / "run", config, client, prefix)
    evidence = tmp_path / "evidence.json"
    evidence.write_text('{"status":"PENDING"}')
    client.objects[prefix + "/raw_events_old.csv"] = b"invalid header\n"
    with pytest.raises(ValueError, match="header"):
        verify_source(tmp_path / "run", config, client, prefix, evidence)
    assert json.loads(evidence.read_text())["status"] == "PENDING"
    assert not (tmp_path / "run/readback.json").exists()
    assert not list(tmp_path.glob(".evidence.json.*"))


def test_evidence_cannot_overwrite_manifest(tmp_path):
    config = yaml.safe_load(Path("config/generator_config.yaml").read_text())
    with pytest.raises(ValueError, match="differ"):
        verify_source(tmp_path, config, MemoryMinIO(), "source/rees46/2019-10/test", tmp_path / "manifest.json")
