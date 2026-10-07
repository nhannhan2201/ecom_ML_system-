"""Streaming CSV-to-message boundary for Kafka Stream Replay.

The iterator is pure CSV-to-message processing; the CLI delegates Kafka I/O to
replay_runtime. Record numbers are provenance, not business event identities.
Date limits are inclusive/exclusive UTC.
"""

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Iterator


SOURCE_COLUMNS = (
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
)


@dataclass(frozen=True)
class ReplayMessage:
    """Serialized message and source metadata for later scheduling/delivery."""

    source_record: int
    event_time: datetime
    key: bytes
    value: bytes


def iter_messages(
    input_csv: str | Path,
    *,
    start: datetime,
    end: datetime,
    discount_values: tuple[int, ...],
    seed: int = 42,
    max_events: int | None = None,
) -> Iterator[ReplayMessage]:
    """Yield at most max_events accepted source records, without read-ahead.

    Invalid records fail fast; valid out-of-range records are excluded. Validate
    non-decreasing timestamps across every record actually read. A bounded call
    does not validate the unconsumed suffix of the file.
    """
    if any(t.tzinfo is None or t.utcoffset().total_seconds() != 0 for t in (start, end)):
        raise ValueError("start/end must be timezone-aware UTC")
    if start >= end:
        raise ValueError("start must be before end")
    if max_events is not None and (type(max_events) is not int or max_events <= 0):
        raise ValueError("max_events must be a positive integer")
    if not discount_values or any(type(v) is not int or not 0 <= v <= 100 for v in discount_values):
        raise ValueError("discount_values must contain integer percentages in [0, 100]")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")

    accepted = 0
    previous = None
    with Path(input_csv).open(encoding="utf-8", newline="") as stream:
        reader = csv.reader(stream, strict=True)
        if next(reader, None) != list(SOURCE_COLUMNS):
            raise ValueError("CSV header must match the nine source columns in order")
        record = 0
        while max_events is None or accepted < max_events:
            try:
                fields = next(reader)
            except StopIteration:
                return
            except csv.Error as exc:
                raise ValueError(f"CSV physical line {reader.line_num}: {exc}") from exc
            record += 1
            try:
                if len(fields) != len(SOURCE_COLUMNS):
                    raise ValueError("expected nine fields")
                payload = dict(zip(SOURCE_COLUMNS, fields))
                raw_time = payload["event_time"]
                if not re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC", raw_time):
                    raise ValueError("event_time must use YYYY-MM-DD HH:MM:SS UTC")
                event_time = datetime.fromisoformat(raw_time[:19]).replace(tzinfo=timezone.utc)
                if previous is not None and event_time < previous:
                    raise ValueError("event_time moved backwards")
                previous = event_time
                for name in ("product_id", "category_id", "user_id"):
                    raw = payload[name]
                    if not re.fullmatch(r"[+-]?\d+", raw):
                        raise ValueError(f"{name} must be an int64")
                    number = int(raw)
                    if not -(2**63) <= number < 2**63:
                        raise ValueError(f"{name} must be an int64")
                    payload[name] = number
                if not payload["event_type"]:
                    raise ValueError("event_type must not be empty")
                payload["price"] = float(payload["price"])
                if not math.isfinite(payload["price"]):
                    raise ValueError("price must be finite")
                for name in ("category_code", "brand", "user_session"):
                    payload[name] = payload[name] or None
                if not start <= event_time < end:
                    continue
                # Record-indexed selection remains reproducible across resume;
                # it does not depend on a mutable random generator's state.
                digest = hashlib.sha256(f"discount:{seed}:{record}".encode()).digest()
                payload["discount_percent"] = discount_values[int.from_bytes(digest, "big") % len(discount_values)]
                message = ReplayMessage(
                    source_record=record,
                    event_time=event_time,
                    key=str(payload["user_id"]).encode("utf-8"),
                    value=json.dumps(payload, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode(
                        "utf-8"
                    ),
                )
            except (ValueError, OverflowError) as exc:
                raise ValueError(f"CSV record {record} (physical line {reader.line_num}): {exc}") from exc
            accepted += 1
            yield message


def main():
    """Load the existing streaming config; Kafka imports stay out of CSV tests."""
    import argparse
    import yaml
    from .replay_runtime import run_replay

    parser = argparse.ArgumentParser(description="November CSV → Kafka with event-time late injection")
    parser.add_argument("--config", default="config/generator_config.yaml")
    parser.add_argument("--run-dir", required=True, help="Fresh local artifact directory; existing only with --resume")
    size = parser.add_mutually_exclusive_group(required=True)
    size.add_argument("--max-events", type=int)
    size.add_argument("--all-events", action="store_true", help="Explicit full-file workload")
    parser.add_argument("--rate", type=float, help="Target Kafka submissions per real second, including duplicates")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--checkpoint-events", type=int, help="Override checkpoint cadence; keep identical on resume")
    args = parser.parse_args()
    with open(args.config, encoding="utf-8") as stream:
        cfg = yaml.safe_load(stream)["streaming_generator"]
    schema, faults = cfg["schema"], cfg["fault_injection"]
    if not schema["include_discount"] or schema["discount_column"] != "discount_percent":
        raise ValueError("Stream payload requires discount_percent")
    if faults.get("burst", {}).get("enabled", False):
        raise ValueError("Burst is outside the approved scope; disable it before replay")
    replay = cfg["replay"]
    late, duplicate = faults["late_arrival"], faults["duplicate"]
    settings = {
        "input_csv": cfg["input_csv"],
        "start": cfg["date_range"]["start_date"] + "T00:00:00+00:00",
        "end": cfg["date_range"]["end_date"] + "T00:00:00+00:00",
        "discount_values": schema["discount_values"],
        "seed": replay["seed"],
        "bootstrap": cfg["kafka"]["bootstrap_servers"],
        "topic": cfg["kafka"]["topic"],
        "max_events": args.max_events,
        "rate": args.rate if args.rate is not None else replay["events_per_second"],
        "late_rate": late["rate"] if late["enabled"] else 0,
        "late_min_seconds": late["delay_minutes_min"] * 60,
        "late_max_seconds": late["delay_minutes_max"] * 60,
        "duplicate_rate": duplicate["rate"] if duplicate["enabled"] else 0,
        "checkpoint_events": args.checkpoint_events
        if args.checkpoint_events is not None
        else replay["checkpoint_events"],
        "pending_limit": replay["pending_limit"],
        "ack_limit": replay["ack_limit"],
        "delivery_timeout": replay["delivery_timeout_seconds"],
    }
    run_replay(settings, args.run_dir, resume=args.resume)


if __name__ == "__main__":
    main()
