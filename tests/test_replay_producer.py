"""CSV boundary correctness; no broker or real dataset required."""

import csv
from datetime import datetime, timezone
import json

import pytest

from src.generator.replay_producer import SOURCE_COLUMNS, iter_messages


START = datetime(2019, 11, 1, tzinfo=timezone.utc)
END = datetime(2019, 12, 1, tzinfo=timezone.utc)
ROW = ["2019-11-01 00:00:00 UTC", "view", "500", "600", "", "", "12.5", "101", "session"]


def source(tmp_path, rows, header=SOURCE_COLUMNS):
    path = tmp_path / "events.csv"
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def messages(path, **kwargs):
    return iter_messages(path, start=START, end=END, discount_values=(4, 5, 8, 10, 12), **kwargs)


def test_exact_schema_types_key_and_repeatability(tmp_path):
    path = source(tmp_path, [ROW, ROW])
    first = list(messages(path))
    assert first == list(messages(path))
    assert [m.source_record for m in first] == [1, 2]
    payload = json.loads(first[0].value)
    assert set(payload) == set(SOURCE_COLUMNS) | {"discount_percent"}
    assert payload == {
        "event_time": ROW[0],
        "event_type": "view",
        "product_id": 500,
        "category_id": 600,
        "category_code": None,
        "brand": None,
        "price": 12.5,
        "user_id": 101,
        "user_session": "session",
        "discount_percent": payload["discount_percent"],
    }
    assert payload["discount_percent"] in (4, 5, 8, 10, 12)
    assert first[0].key == b"101"
    assert first[0].event_time == START


def test_interval_boundaries_and_accepted_limit(tmp_path):
    rows = []
    for timestamp in ("2019-10-31 23:59:59 UTC", ROW[0], "2019-11-30 23:59:59 UTC", "2019-12-01 00:00:00 UTC"):
        row = ROW.copy()
        row[0] = timestamp
        rows.append(row)
    path = source(tmp_path, rows)
    assert [m.source_record for m in messages(path)] == [2, 3]
    assert [m.source_record for m in messages(path, max_events=1)] == [2]


def test_bounded_iteration_does_not_parse_suffix(tmp_path):
    path = source(tmp_path, [ROW, ["broken"]])
    assert len(list(messages(path, max_events=1))) == 1
    iterator = messages(path)
    assert next(iterator).key == b"101"
    with pytest.raises(ValueError, match="record 2.*nine fields"):
        next(iterator)


@pytest.mark.parametrize(
    "column,value",
    [
        ("event_time", "2019-11-31 00:00:00 UTC"),
        ("event_time", "2019-11-01T00:00:00Z"),
        ("user_id", ""),
        ("user_id", "1.5"),
        ("product_id", str(2**63)),
        ("category_id", "oops"),
        ("price", "NaN"),
        ("price", "Infinity"),
        ("price", ""),
        ("event_type", ""),
    ],
)
def test_invalid_records_fail_with_source_location(tmp_path, column, value):
    row = ROW.copy()
    row[SOURCE_COLUMNS.index(column)] = value
    with pytest.raises(ValueError, match="CSV record 1 .*physical line 2"):
        list(messages(source(tmp_path, [row])))


def test_timestamp_regression_fails_but_ties_are_allowed(tmp_path):
    later = ROW.copy()
    later[0] = "2019-11-01 00:00:01 UTC"
    with pytest.raises(ValueError, match="record 3.*backwards"):
        list(messages(source(tmp_path, [later, later, ROW])))


@pytest.mark.parametrize(
    "rows,header", [([], ()), ([ROW], tuple(reversed(SOURCE_COLUMNS))), ([ROW + ["extra"]], SOURCE_COLUMNS)]
)
def test_structural_errors(tmp_path, rows, header):
    with pytest.raises(ValueError):
        list(messages(source(tmp_path, rows, header)))


def test_header_only_file(tmp_path):
    assert list(messages(source(tmp_path, []))) == []


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_invalid_max_events(tmp_path, limit):
    with pytest.raises(ValueError, match="max_events"):
        list(messages(source(tmp_path, [ROW]), max_events=limit))


def test_csv_quoting_and_utf8(tmp_path):
    row = ROW.copy()
    row[5] = 'nhãn, "hiệu"'
    payload = json.loads(next(messages(source(tmp_path, [row]))).value)
    assert payload["brand"] == row[5]


def test_invalid_boundaries_and_discount_config(tmp_path):
    path = source(tmp_path, [ROW])
    for start, end, discounts in (
        (END, START, (4,)),
        (START.replace(tzinfo=None), END, (4,)),
        (START, END, ()),
        (START, END, (True,)),
        (START, END, (101,)),
    ):
        with pytest.raises(ValueError):
            list(iter_messages(path, start=start, end=end, discount_values=discounts))
