"""Scheduler, ACK/recovery and bounded readback tests with isolated fake clients."""

import csv
from datetime import datetime, timezone
import json
import sys
from types import SimpleNamespace

import pytest

from src.generator.replay_producer import SOURCE_COLUMNS, ReplayMessage
from src.generator.replay_runtime import Delivery, Scheduler, run_replay, validate_settings
from scripts.verify_kafka_replay import verify_records


def settings(tmp_path, **overrides):
    path = tmp_path / "events.csv"
    if not path.exists():
        with path.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(SOURCE_COLUMNS)
            for timestamp in ("00:00:00", "00:01:00", "00:06:00", "00:12:00"):
                writer.writerow([f"2019-11-01 {timestamp} UTC", "view", "1", "2", "", "", "1.5", "101", "s"])
    return {
        "input_csv": str(path),
        "start": "2019-11-01T00:00:00+00:00",
        "end": "2019-12-01T00:00:00+00:00",
        "discount_values": [4, 5],
        "seed": 42,
        "bootstrap": "localhost:9092",
        "topic": "test",
        "max_events": 4,
        "rate": 1000000,
        "late_rate": 1,
        "duplicate_rate": 1,
        "late_min_seconds": 300,
        "late_max_seconds": 300,
        "pending_limit": 100,
        "checkpoint_events": 1,
        "ack_limit": 100,
        "delivery_timeout": 1,
        **overrides,
    }


def message(record, seconds):
    return ReplayMessage(record, datetime.fromtimestamp(1572566400 + seconds, timezone.utc), b"101", b"{}")


class FakeMessage:
    def __init__(self, partition, offset, key=b"101", value=b"{}", headers=None):
        self.p, self.o, self.k, self.v, self.h = partition, offset, key, value, headers

    def partition(self):
        return self.p

    def offset(self):
        return self.o

    def key(self):
        return self.k

    def value(self):
        return self.v

    def headers(self):
        return self.h

    def error(self):
        return None


class FakeProducer:
    def __init__(self, conf, storage=None, fail_at=None):
        assert conf["enable.idempotence"] and conf["acks"] == "all"
        assert conf["allow.auto.create.topics"] is False
        self.storage = storage if storage is not None else []
        self.callbacks = []
        self.fail_at = fail_at
        self.calls = 0

    def produce(self, topic, key, value, headers, on_delivery):
        self.calls += 1
        if self.calls == self.fail_at:
            raise RuntimeError("simulated crash")
        item = FakeMessage(0, len(self.storage), key, value, [(k, v.encode()) for k, v in headers.items()])
        self.storage.append(item)
        self.callbacks.append((on_delivery, item))

    def poll(self, timeout):
        if self.callbacks:
            callback, item = self.callbacks.pop(0)
            callback(None, item)

    def flush(self, timeout):
        while self.callbacks:
            self.poll(0)
        return 0


IDENTITY = {"cluster_id": "cluster", "topic": "test", "topic_id": "uuid", "partitions": [0]}


def test_late_by_event_progress_and_end_flush(tmp_path):
    scheduler = Scheduler(settings(tmp_path))
    assert scheduler.accept(message(1, 0)) == []
    assert scheduler.accept(message(2, 60)) == []
    ready = scheduler.accept(message(3, 360))
    assert [(entry["record"], release) for entry, release in ready] == [(1, "due"), (2, "due")]
    restored = Scheduler(settings(tmp_path), scheduler.snapshot())
    assert [(entry["record"], release) for entry, release in restored.finish()] == [(3, "end_flush")]


def test_no_injection_and_queue_cap(tmp_path):
    scheduler = Scheduler(settings(tmp_path, late_rate=0, duplicate_rate=0))
    entry, release = scheduler.accept(message(1, 0))[0]
    assert release == "normal" and entry["copies"] == 1
    scheduler = Scheduler(settings(tmp_path, pending_limit=1))
    scheduler.accept(message(1, 0))
    with pytest.raises(RuntimeError, match="capacity"):
        scheduler.accept(message(2, 1))


def test_run_and_exact_readback(tmp_path, monkeypatch):
    storage = []
    summary = run_replay(
        settings(tmp_path),
        tmp_path / "run",
        producer_factory=lambda conf: FakeProducer(conf, storage),
        identity_reader=lambda: IDENTITY,
    )
    assert summary["counters"] == {
        "accepted": 4,
        "submitted": 8,
        "acked": 8,
        "audit_records": 8,
        "duplicate_messages": 4,
        "late_selected_events": 4,
        "late_due_events": 3,
        "late_end_flush_events": 1,
    }
    assert summary["audit_scope"] == "all"
    state = json.loads((tmp_path / "run/checkpoint.json").read_text())
    assert state["complete"] and not state["pending"]
    expected = [
        {**json.loads(line), "topic": "test"} for line in (tmp_path / "run/acks.jsonl").read_text().splitlines()
    ]
    # No Kafka dependency in tests: only the TopicPartition data holder is used.
    monkeypatch.setitem(sys.modules, "confluent_kafka", SimpleNamespace(TopicPartition=lambda *args: args))
    consumer = FakeConsumer(storage)
    assert verify_records(consumer, expected, 1) == 8
    storage[0].k = b"wrong"
    with pytest.raises(RuntimeError, match="mismatch"):
        verify_records(FakeConsumer(storage), expected, 1)


class FakeConsumer:
    def __init__(self, messages):
        self.messages = list(messages)

    def get_watermark_offsets(self, partition, timeout):
        return (0, len(self.messages))

    def assign(self, partitions):
        pass

    def pause(self, partitions):
        pass

    def poll(self, timeout):
        return self.messages.pop(0) if self.messages else None


def test_resume_preserves_pending_and_may_repeat_uncheckpointed_ack(tmp_path):
    cfg = settings(tmp_path)
    storage = []
    directory = tmp_path / "run"
    with pytest.raises(RuntimeError, match="simulated crash"):
        run_replay(
            cfg,
            directory,
            producer_factory=lambda conf: FakeProducer(conf, storage, fail_at=2),
            identity_reader=lambda: IDENTITY,
        )
    state = json.loads((directory / "checkpoint.json").read_text())
    assert state["last_record"] == 2
    assert len(state["pending"]) == 2
    assert len(storage) == 1  # ACK already happened, but its source progress was not checkpointed.
    summary = run_replay(
        cfg,
        directory,
        resume=True,
        producer_factory=lambda conf: FakeProducer(conf, storage),
        identity_reader=lambda: IDENTITY,
    )
    assert summary["counters"]["acked"] == 8
    assert len(storage) == 9  # at-least-once, not exactly-once across restart
    assert len((directory / "acks.jsonl").read_text().splitlines()) == 8
    with pytest.raises(ValueError, match="already complete"):
        run_replay(cfg, directory, resume=True, producer_factory=FakeProducer, identity_reader=lambda: IDENTITY)


def test_resume_refuses_changed_topic_or_config(tmp_path):
    cfg = settings(tmp_path, late_rate=0)
    directory = tmp_path / "run"
    with pytest.raises(RuntimeError):
        run_replay(
            cfg,
            directory,
            producer_factory=lambda conf: FakeProducer(conf, fail_at=1),
            identity_reader=lambda: IDENTITY,
        )
    for new_cfg, new_identity in (({**cfg, "seed": 1}, IDENTITY), (cfg, {**IDENTITY, "topic_id": "new"})):
        with pytest.raises(ValueError, match="identity changed"):
            run_replay(
                new_cfg, directory, resume=True, producer_factory=FakeProducer, identity_reader=lambda: new_identity
            )


def test_audit_cap_is_sample_only(tmp_path):
    summary = run_replay(
        settings(tmp_path, ack_limit=2),
        tmp_path / "run",
        producer_factory=FakeProducer,
        identity_reader=lambda: IDENTITY,
    )
    assert summary["audit_scope"] == "sample_only"
    assert summary["counters"]["audit_records"] == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("rate", 0),
        ("rate", float("nan")),
        ("late_rate", 1.1),
        ("duplicate_rate", -1),
        ("pending_limit", 0),
        ("max_events", 0),
        ("late_min_seconds", 601),
    ],
)
def test_bad_settings(tmp_path, field, value):
    with pytest.raises(ValueError):
        validate_settings(settings(tmp_path, **{field: value}))


def test_failed_ack_and_flush_timeout_do_not_checkpoint(tmp_path):
    class Failed(FakeProducer):
        def poll(self, timeout):
            if self.callbacks:
                callback, item = self.callbacks.pop(0)
                callback("delivery error", item)

    class Stuck(FakeProducer):
        def poll(self, timeout):
            pass

        def flush(self, timeout):
            return 1

    for index, factory in enumerate((Failed, Stuck)):
        directory = tmp_path / f"run-{index}"
        with pytest.raises(RuntimeError):
            run_replay(
                settings(tmp_path, late_rate=0), directory, producer_factory=factory, identity_reader=lambda: IDENTITY
            )
        state = json.loads((directory / "checkpoint.json").read_text())
        assert state["last_record"] == 0 and not state["complete"]


def test_rate_and_transient_queue_backpressure(tmp_path):
    cfg = settings(tmp_path, rate=2, late_rate=0)
    entry, release = Scheduler(cfg).accept(message(1, 0))[0]

    class Clock:
        now = 0

        def time(self):
            return self.now

        def sleep(self, delay):
            self.now += delay

    class BusyOnce(FakeProducer):
        busy = True

        def produce(self, *args, **kwargs):
            if self.busy:
                self.busy = False
                raise BufferError("queue full")
            return super().produce(*args, **kwargs)

    clock = Clock()
    producer = BusyOnce({"acks": "all", "enable.idempotence": True, "allow.auto.create.topics": False})
    counters = {
        key: 0
        for key in (
            "submitted",
            "acked",
            "audit_records",
            "duplicate_messages",
            "late_due_events",
            "late_end_flush_events",
        )
    }
    with (tmp_path / "audit.jsonl").open("w") as stream:
        delivery = Delivery(producer, cfg, "run", stream, counters, clock.time, clock.sleep)
        delivery.send(entry, release, "2019-11-01T00:00:00+00:00")
        delivery.drain()
    assert clock.now == 0.5  # Two deliberate copies at two submissions/second.
    assert counters["acked"] == 2


def test_full_late_queue_releases_due_before_capacity_check(tmp_path):
    scheduler = Scheduler(settings(tmp_path, pending_limit=1))
    scheduler.accept(message(1, 0))
    ready = scheduler.accept(message(2, 300))
    assert ready[0][0]["record"] == 1
    assert len(scheduler.pending) == 1


def test_readback_rejects_missing_and_expired_positions(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "confluent_kafka", SimpleNamespace(TopicPartition=lambda *args: args))
    row = {"topic": "test", "partition": 0, "offset": 0, "key": "101", "value": "{}", "headers": {}}
    with pytest.raises(RuntimeError, match="unavailable"):
        verify_records(FakeConsumer([]), [row], 1)

    class Missing(FakeConsumer):
        def get_watermark_offsets(self, partition, timeout):
            return (0, 1)

    with pytest.raises(RuntimeError, match="incomplete"):
        verify_records(Missing([]), [row], 0.01)


def test_readback_rejects_early_late_release(monkeypatch):
    monkeypatch.setitem(sys.modules, "confluent_kafka", SimpleNamespace(TopicPartition=lambda *args: args))
    headers = {"release": "due", "progress": "2019-11-01T00:01:00+00:00", "delay_seconds": "300"}
    value = json.dumps({"event_time": "2019-11-01 00:00:00 UTC"})
    row = {"topic": "test", "partition": 0, "offset": 0, "key": "101", "value": value, "headers": headers}
    item = FakeMessage(0, 0, b"101", value.encode(), [(k, v.encode()) for k, v in headers.items()])
    with pytest.raises(RuntimeError, match="before event-time threshold"):
        verify_records(FakeConsumer([item]), [row], 1)


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
def test_readback_requires_finite_positive_timeout(monkeypatch, timeout):
    monkeypatch.setitem(sys.modules, "confluent_kafka", SimpleNamespace(TopicPartition=lambda *args: args))
    with pytest.raises(ValueError, match="finite and positive"):
        verify_records(FakeConsumer([]), [], timeout)
