#!/usr/bin/env python3
"""
================================================================================
SCRIPT: PROFILE STREAM DATA (EVALUATE STREAMING FAULT INJECTION & METRICS)
Project: E-Commerce Real-Time Purchase Propensity Prediction System
Author: Hoang Minh Nhan

Purpose:
  Reads data/stream_manifest.json and produces docs/evidence/stream_profile.md
  summarizing late arrival distribution, duplicates, burst throughput, and volume.
================================================================================
"""

import os
import sys
import json
import logging
import subprocess
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("StreamProfiler")


def get_git_commit() -> str:
    """Get current git commit hash."""
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"]).decode("utf-8").strip()
    except Exception:
        return "unknown"


def main():
    manifest_path = "data/stream_manifest.json"
    if not os.path.exists(manifest_path):
        logger.error(f"Khong tim thay manifest tai '{manifest_path}'. Hay chay stream generator truoc.")
        sys.exit(1)

    with open(manifest_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    git_commit = get_git_commit()
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    topic = data.get("topic", "ecommerce_stream_events")
    total_produced = data.get("total_produced", 0)
    normal_produced = data.get("normal_produced", 0)
    dup_count = data.get("duplicates_injected", 0)
    dup_rate = data.get("duplicate_rate_actual", 0.0)
    late_delayed = data.get("late_delayed", 0)
    late_released = data.get("late_released", 0)
    delays = data.get("delay_distribution_minutes", {})
    burst_events = data.get("burst_events", 0)
    burst_cfg = data.get("burst_config", {})
    elapsed = data.get("elapsed_seconds", 0.0)
    throughput = data.get("average_throughput_msg_per_sec", 0.0)
    last_event_time = data.get("last_event_time", "N/A")

    late_pct = round((late_delayed / max(1, total_produced)) * 100, 2)

    lines = [
        "# Bao Cao Do Luong Du Lieu Streaming (Streaming Profile Evidence)",
        "",
        f"> **Thoi diem tao**: `{timestamp}`  ",
        f"> **Git commit**: `{git_commit}`  ",
        "> **Nguon du lieu**: `data/stream_manifest.json` tu Kafka Stream Generator.",
        "",
        "---",
        "",
        "## 1. Tong Quan Thong Luong va Quy Mo (Volume & Throughput)",
        "",
        "| Chi So | Gia Tri Do Duoc | Ghi Chu |",
        "| :--- | :--- | :--- |",
        f"| **Topic Kafka** | `{topic}` | 3 partitions, key=`user_id` |",
        f"| **Tong so su kien da gui** | {total_produced:,} messages | Bao gom normal, dup, late |",
        f"| **So su kien tieu chuan** | {normal_produced:,} messages | Luong binh thuong |",
        f"| **Thoi gian chay** | {elapsed:.2f} giay | - |",
        f"| **Thong luong trung binh** | {throughput:.1f} messages/giay | - |",
        f"| **Event Time cuoi cung** | `{last_event_time}` | Tien trinh event time thuc |",
        "",
        "---",
        "",
        "## 2. Minh Chung Late Arrival (Loi Streaming 2 - Rubric DE)",
        "",
        "| Thong So Do Tre | Gia Tri Thuc Nghiem | Muc Tieu Cau Hinh |",
        "| :--- | :--- | :--- |",
        f"| **So su kien bi tre** | {late_delayed:,} events | - |",
        f"| **So su kien da giai phong** | {late_released:,} events | Theo event-time buffer |",
        f"| **Ty le su kien tre** | **{late_pct:.2f}%** | ~5.00% |",
        f"| **Do tre toi thieu (Min)** | {delays.get('min', 0.0)} phut | >= 5 phut |",
        f"| **Do tre trung vi (Median)** | {delays.get('median', 0.0)} phut | 5 - 10 phut |",
        f"| **Do tre toi da (Max)** | {delays.get('max', 0.0)} phut | <= 10 phut |",
        f"| **Do tre trung binh (Mean)** | {delays.get('mean', 0.0)} phut | - |",
        "",
        "---",
        "",
        "## 3. Minh Chung Duplicate Records (Loi Streaming 3 - Rubric DE)",
        "",
        "| Hang Muc | Gia Tri Thuc Nghiem | Muc Tieu Cau Hinh |",
        "| :--- | :--- | :--- |",
        f"| **So duplicate tiem vao** | {dup_count:,} messages | Nhan ban x2 goi tin |",
        f"| **Ty le duplicate thuc te** | **{dup_rate:.2f}%** | ~1.50% |",
        "",
        "---",
        "",
        "## 4. Minh Chung Burst Traffic (Loi Streaming 1 - Rubric DE)",
        "",
        "| Cau Hinh Flash Sale | Gia Tri Thuc Te | Mo Ta |",
        "| :--- | :--- | :--- |",
        f"| **He so tang dot bien** | x{burst_cfg.get('multiplier', 10)} | Tang dot bien throughput |",
        f"| **Thoi luong burst** | {burst_cfg.get('duration_seconds', 600)} giay | Chu ky flash sale |",
        f"| **So su kien trong pha burst** | {burst_events:,} messages | - |",
        ""
    ]

    out_md = "docs/evidence/stream_profile.md"
    os.makedirs(os.path.dirname(out_md), exist_ok=True)
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    out_json = "docs/evidence/stream_profile.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print("\n" + "=" * 80)
    print("HOAN TAT DO LUONG DU LIEU STREAMING")
    print(f"   - Markdown: {out_md}")
    print(f"   - JSON:     {out_json}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
