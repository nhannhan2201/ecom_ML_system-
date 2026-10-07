"""Kafka delivery, event-time scheduling and local recovery for Stream Replay."""

import hashlib
import heapq
import json
import math
import os
from pathlib import Path
import time
import uuid

from .replay_producer import iter_messages


def fraction(seed, record, purpose):
    digest = hashlib.sha256(f"{purpose}:{seed}:{record}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def atomic_json(path, value):
    """Replace only this run's state; callers hold its exclusive run lock."""
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, allow_nan=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def topic_identity(admin, topic):
    from confluent_kafka import TopicCollection

    cluster = admin.describe_cluster(request_timeout=10).result(timeout=15)
    description = admin.describe_topics(TopicCollection([topic]), request_timeout=10)[topic].result(timeout=15)
    return {
        "cluster_id": cluster.cluster_id,
        "topic": topic,
        "topic_id": str(description.topic_id),
        "partitions": sorted(p.id for p in description.partitions),
    }


def validate_settings(settings):
    for name in ("rate", "delivery_timeout"):
        if (
            not isinstance(settings[name], (int, float))
            or isinstance(settings[name], bool)
            or not math.isfinite(settings[name])
            or settings[name] <= 0
        ):
            raise ValueError(f"{name} must be finite and positive")
    for name in ("checkpoint_events", "pending_limit", "ack_limit"):
        if type(settings[name]) is not int or settings[name] <= 0:
            raise ValueError(f"{name} must be a positive integer")
    for name in ("late_rate", "duplicate_rate"):
        if (
            not isinstance(settings[name], (int, float))
            or isinstance(settings[name], bool)
            or not math.isfinite(settings[name])
            or not 0 <= settings[name] <= 1
        ):
            raise ValueError(f"{name} must be in [0, 1]")
    low, high = settings["late_min_seconds"], settings["late_max_seconds"]
    if any(type(v) is not int or v < 0 for v in (low, high)) or low > high:
        raise ValueError("late delay must be ordered non-negative integer seconds")
    if settings["late_rate"] and low == 0:
        raise ValueError("enabled late injection requires positive delay")
    if settings["max_events"] is not None and (type(settings["max_events"]) is not int or settings["max_events"] <= 0):
        raise ValueError("max_events must be positive or None")


class Scheduler:
    """Hold selected source events until source progress reaches their due time."""

    def __init__(self, settings, pending=()):
        self.settings = settings
        self.pending = [(entry["due"], entry["record"], entry) for entry in pending]
        heapq.heapify(self.pending)

    def accept(self, message):
        s = self.settings
        late = fraction(s["seed"], message.source_record, "late") < s["late_rate"]
        delay = 0
        if late:
            width = s["late_max_seconds"] - s["late_min_seconds"] + 1
            delay = s["late_min_seconds"] + min(
                width - 1, int(fraction(s["seed"], message.source_record, "delay") * width)
            )
        entry = {
            "record": message.source_record,
            "event_time": message.event_time.isoformat(),
            "key": message.key.decode("utf-8"),
            "value": message.value.decode("utf-8"),
            "due": message.event_time.timestamp() + delay,
            "delay_seconds": delay,
            "copies": 2 if fraction(s["seed"], message.source_record, "duplicate") < s["duplicate_rate"] else 1,
        }
        # Immediate event is emitted before due delayed events: readback can
        # witness progress reaching the delayed event's release threshold.
        due = []
        while self.pending and self.pending[0][0] <= message.event_time.timestamp():
            due.append((heapq.heappop(self.pending)[2], "due"))
        ready = []
        if late:
            if len(self.pending) >= s["pending_limit"]:
                raise RuntimeError(
                    "Late queue capacity exceeded; increase pending_limit or lower late rate. No events dropped."
                )
            heapq.heappush(self.pending, (entry["due"], entry["record"], entry))
        else:
            ready.append((entry, "normal"))
        return ready + due

    def finish(self):
        while self.pending:
            yield heapq.heappop(self.pending)[2], "end_flush"

    def snapshot(self):
        return [entry for _, _, entry in sorted(self.pending)]


class Delivery:
    """Asynchronous delivery with bounded backpressure and explicit ACK audit."""

    def __init__(self, producer, settings, run_id, journal, counters, clock=time.monotonic, sleep=time.sleep):
        self.producer, self.settings = producer, settings
        self.run_id, self.journal, self.counters = run_id, journal, counters
        self.clock, self.sleep = clock, sleep
        self.next_send = clock()
        self.errors = []

    def check(self):
        if self.errors:
            raise RuntimeError(f"Kafka delivery failed: {self.errors[0]}")

    def send(self, entry, release, progress):
        for copy in range(entry["copies"]):
            self.check()
            delay = self.next_send - self.clock()
            if delay > 0:
                self.sleep(delay)
            self.next_send = max(self.next_send, self.clock()) + 1 / self.settings["rate"]
            headers = {
                "replay_run": self.run_id,
                "source_record": str(entry["record"]),
                "copy": str(copy),
                "release": release,
                "delay_seconds": str(entry["delay_seconds"]),
                "progress": progress,
            }
            expected = {"key": entry["key"], "value": entry["value"], "headers": headers}

            def delivered(error, message, expected=expected):
                if error is not None:
                    self.errors.append(str(error))
                    return
                self.counters["acked"] += 1
                if self.counters["audit_records"] < self.settings["ack_limit"]:
                    self.journal.write(
                        json.dumps(
                            {**expected, "partition": message.partition(), "offset": message.offset()},
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    self.counters["audit_records"] += 1

            deadline = self.clock() + self.settings["delivery_timeout"]
            while True:
                try:
                    self.producer.produce(
                        self.settings["topic"],
                        key=entry["key"].encode(),
                        value=entry["value"].encode(),
                        headers=headers,
                        on_delivery=delivered,
                    )
                    break
                except BufferError:
                    self.producer.poll(0.1)
                    self.check()
                    if self.clock() >= deadline:
                        raise RuntimeError("Kafka queue remained full until delivery deadline")
            self.counters["submitted"] += 1
            if copy:
                self.counters["duplicate_messages"] += 1
            self.producer.poll(0)
            self.check()
        if release == "due":
            self.counters["late_due_events"] += 1
        elif release == "end_flush":
            self.counters["late_end_flush_events"] += 1

    def drain(self):
        remaining = self.producer.flush(self.settings["delivery_timeout"])
        self.check()
        if remaining or self.counters["submitted"] != self.counters["acked"]:
            raise RuntimeError(f"Unconfirmed delivery: remaining={remaining}")
        self.journal.flush()
        os.fsync(self.journal.fileno())


def run_replay(settings, run_dir, resume=False, producer_factory=None, identity_reader=None):
    """Only checkpoints after ACK drain; persists pending late events atomically.

    Crash after Kafka ACK but before checkpoint may repeat uncheckpointed sends.
    Idempotence does not provide cross-process exactly-once delivery.
    """
    import fcntl
    from datetime import datetime

    validate_settings(settings)
    source = Path(settings["input_csv"]).resolve()
    stat = source.stat()
    binding = {**settings, "input_csv": str(source), "source_size": stat.st_size, "source_mtime_ns": stat.st_mtime_ns}
    if identity_reader is None:
        from confluent_kafka.admin import AdminClient

        def identity_reader():
            return topic_identity(AdminClient({"bootstrap.servers": settings["bootstrap"]}), settings["topic"])

    identity = identity_reader()  # Missing topic fails; never auto-create.
    directory = Path(run_dir)
    if resume:
        if not directory.is_dir():
            raise ValueError("Resume directory does not exist")
    else:
        directory.mkdir(parents=True, exist_ok=False)
    with (directory / "lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        checkpoint_path = directory / "checkpoint.json"
        if resume:
            state = json.loads(checkpoint_path.read_text())
            if state["binding"] != binding or state["identity"] != identity:
                raise ValueError("Source/config or Kafka topic identity changed; refusing resume")
            if state["complete"]:
                raise ValueError("Run already complete; use a fresh run directory")
        else:
            state = {
                "binding": binding,
                "identity": identity,
                "run_id": str(uuid.uuid4()),
                "last_record": 0,
                "progress": None,
                "pending": [],
                "journal_bytes": 0,
                "complete": False,
                "elapsed_seconds": 0,
                "counters": {
                    name: 0
                    for name in (
                        "accepted",
                        "submitted",
                        "acked",
                        "audit_records",
                        "duplicate_messages",
                        "late_selected_events",
                        "late_due_events",
                        "late_end_flush_events",
                    )
                },
            }
            atomic_json(checkpoint_path, state)
        if producer_factory is None:
            from confluent_kafka import Producer

            producer_factory = Producer
        producer = producer_factory(
            {
                "bootstrap.servers": settings["bootstrap"],
                "acks": "all",
                "enable.idempotence": True,
                "allow.auto.create.topics": False,
                "delivery.timeout.ms": int(settings["delivery_timeout"] * 1000),
                "queue.buffering.max.messages": 10000,
            }
        )
        journal_path = directory / "acks.jsonl"
        mode = "r+" if journal_path.exists() else "w+"
        with journal_path.open(mode, encoding="utf-8") as journal:
            # Discard only this run's uncheckpointed audit suffix on recovery.
            if journal_path.stat().st_size < state["journal_bytes"]:
                raise ValueError("ACK journal is shorter than its checkpoint")
            journal.seek(state["journal_bytes"])
            journal.truncate()
            counters = state["counters"]
            scheduler = Scheduler(settings, state["pending"])
            delivery = Delivery(producer, settings, state["run_id"], journal, counters)
            started = time.monotonic()
            previous_elapsed = state["elapsed_seconds"]

            def checkpoint(complete=False):
                delivery.drain()
                current = source.stat()
                if (stat.st_size, stat.st_mtime_ns) != (current.st_size, current.st_mtime_ns):
                    raise RuntimeError("Source changed during replay")
                state.update(
                    pending=scheduler.snapshot(),
                    journal_bytes=journal.buffer.tell(),
                    complete=complete,
                    elapsed_seconds=previous_elapsed + time.monotonic() - started,
                )
                atomic_json(checkpoint_path, state)
                print(
                    json.dumps(
                        {"checkpoint_record": state["last_record"], "pending_late": len(scheduler.pending), **counters}
                    ),
                    flush=True,
                )

            iterator = iter_messages(
                source,
                start=datetime.fromisoformat(settings["start"]),
                end=datetime.fromisoformat(settings["end"]),
                discount_values=tuple(settings["discount_values"]),
                seed=settings["seed"],
                max_events=settings["max_events"],
            )
            for message in iterator:
                if message.source_record <= state["last_record"]:
                    continue
                ready = scheduler.accept(message)
                progress = message.event_time.isoformat()
                counters["accepted"] += 1
                counters["late_selected_events"] += int(
                    fraction(settings["seed"], message.source_record, "late") < settings["late_rate"]
                )
                for entry, release in ready:
                    delivery.send(entry, release, progress)
                state["last_record"], state["progress"] = message.source_record, progress
                if counters["accepted"] % settings["checkpoint_events"] == 0:
                    checkpoint()
            for entry, release in scheduler.finish():
                delivery.send(entry, release, state["progress"])
            checkpoint(complete=True)
            elapsed = time.monotonic() - started
            summary = {
                "status": "ACK_PASS",
                "run_id": state["run_id"],
                "identity": identity,
                "counters": counters,
                "elapsed_this_process_seconds": elapsed,
                "elapsed_checkpointed_seconds": state["elapsed_seconds"],
                "acked_per_second": counters["acked"] / max(state["elapsed_seconds"], 1e-9),
                "audit_scope": "all" if counters["audit_records"] == counters["acked"] else "sample_only",
                "verification": "NOT_RUN",
                "restart_semantics": "at-least-once",
            }
            atomic_json(directory / "summary.json", summary)
            print(json.dumps(summary), flush=True)
            return summary
