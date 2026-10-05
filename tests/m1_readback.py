"""Manual M1 readback: python tests/m1_readback.py OUTPUT_DIR (local reads only)."""
import argparse
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


def readback(output_dir):
    manifest = json.loads((output_dir / "generation_manifest.json").read_text())
    start = pd.Timestamp("2019-10-01T00:00:00Z")
    effective = pd.Timestamp("2019-10-16T00:00:00Z")
    end = pd.Timestamp("2019-11-01T00:00:00Z")
    original_columns = ["event_time", "event_type", "product_id", "category_id", "category_code",
                        "brand", "price", "user_id", "user_session"]
    result = {
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "evidence_level": "SMALL-RUNTIME-VERIFIED",
        "versions": {"python": platform.python_version(), "pandas": pd.__version__, "numpy": np.__version__},
        "source_classification_counts": manifest["source_classification_counts"],
        "selected_source_counts": manifest["selected_source_counts"],
        "outputs": {},
    }
    for group, lower, upper in [("OLD", start, effective), ("NEW", effective, end)]:
        path = output_dir / f"raw_events_{group.lower()}.csv"
        frame = pd.read_csv(path)
        # Independent timestamp parser: do not reuse production classifier.
        timestamps = pd.to_datetime(frame.event_time.str.removesuffix(" UTC"), format="ISO8601", utc=True)
        assert len(frame) > 0
        assert timestamps.notna().all()
        assert (timestamps.ge(lower) & timestamps.lt(upper)).all(), group
        expected_columns = original_columns + (["discount_percent"] if group == "NEW" else [])
        assert list(frame.columns) == expected_columns, group
        result["outputs"][group] = {
            "path": str(path), "rows_after_transformation": len(frame),
            "min_event_timestamp": timestamps.min().isoformat(),
            "max_event_timestamp": timestamps.max().isoformat(),
            "columns": list(frame.columns), "wrong_schema_membership_rows": 0,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    assert sum(item["rows_after_transformation"] for item in result["outputs"].values()) == manifest["total_rows"]
    assert manifest["selected_source_counts"] == {"OLD": 500, "NEW": 500}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", type=Path)
    readback(parser.parse_args().output_dir)
