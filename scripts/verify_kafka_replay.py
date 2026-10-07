"""Bounded ACK-position readback; never commits offsets or mutates topics."""

import argparse
from datetime import datetime
import json
import math
from pathlib import Path
import time


def verify_records(consumer, expected, timeout=30):
    """Compare exact bytes/headers at ACK positions; consume only a bounded range."""
    from confluent_kafka import TopicPartition

    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    if not expected:
        raise ValueError("No ACK audit records to verify")
    targets = {}
    for row in expected:
        position = (row["partition"], row["offset"])
        if position in targets:
            raise ValueError("Duplicate ACK position in journal")
        targets[position] = row
    topic = expected[0]["topic"]
    bounds = {}
    for partition, offset in targets:
        bounds.setdefault(partition, []).append(offset)
    for partition, offsets in bounds.items():
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        if min(offsets) < low or max(offsets) >= high:
            raise RuntimeError("ACK positions are unavailable (retention/truncation); cannot verify")
    consumer.assign([TopicPartition(topic, p, min(offsets)) for p, offsets in bounds.items()])
    seen, finished = set(), set()
    deadline = time.monotonic() + timeout
    while len(seen) < len(targets) and time.monotonic() < deadline:
        message = consumer.poll(min(1, max(0, deadline - time.monotonic())))
        if message is None:
            continue
        if message.error():
            raise RuntimeError(message.error())
        position = message.partition(), message.offset()
        if position in targets:
            row = targets[position]
            headers = {k: v.decode("utf-8") if v is not None else None for k, v in message.headers() or []}
            if (
                message.key() != row["key"].encode()
                or message.value() != row["value"].encode()
                or headers != row["headers"]
            ):
                raise RuntimeError(f"Message mismatch at {position}")
            if headers["release"] == "due":
                payload = json.loads(message.value())
                event = datetime.strptime(payload["event_time"], "%Y-%m-%d %H:%M:%S UTC")
                progress = datetime.fromisoformat(headers["progress"]).replace(tzinfo=None)
                if (progress - event).total_seconds() < int(headers["delay_seconds"]):
                    raise RuntimeError("Late event released before event-time threshold")
            seen.add(position)
        partition = message.partition()
        if partition in bounds and message.offset() >= max(bounds[partition]) and partition not in finished:
            consumer.pause([TopicPartition(topic, partition)])
            finished.add(partition)
    if len(seen) != len(targets):
        raise RuntimeError(f"Readback incomplete: {len(seen)}/{len(targets)}")
    return len(seen)


def main():
    from confluent_kafka import Consumer
    from confluent_kafka.admin import AdminClient
    from src.generator.replay_runtime import atomic_json, topic_identity

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    directory = Path(args.run_dir)
    state = json.loads((directory / "checkpoint.json").read_text())
    if not state["complete"]:
        raise ValueError("Replay is incomplete; resume it before final verification")
    settings = state["binding"]
    actual = topic_identity(AdminClient({"bootstrap.servers": settings["bootstrap"]}), settings["topic"])
    if actual != state["identity"]:
        raise ValueError("Kafka topic identity changed")
    with (directory / "acks.jsonl").open(encoding="utf-8") as stream:
        expected = [{**json.loads(line), "topic": settings["topic"]} for line in stream]
    if len(expected) != state["counters"]["audit_records"]:
        raise ValueError("ACK audit length mismatch")
    consumer = Consumer(
        {
            "bootstrap.servers": settings["bootstrap"],
            "group.id": "replay-verify-" + state["run_id"],
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )
    try:
        count = verify_records(consumer, expected, args.timeout)
    finally:
        consumer.close()
    result = {
        "status": "READBACK_PASS",
        "verified_messages": count,
        "scope": "all" if count == state["counters"]["acked"] else "sample_only",
        "late_due_events": state["counters"]["late_due_events"],
        "late_end_flush_events": state["counters"]["late_end_flush_events"],
        "note": "end_flush is not evidence of full configured late delay; restart can add uncheckpointed duplicates",
    }
    atomic_json(directory / "readback.json", result)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
