"""Evidence Checklist Auditor.

Scans docs/EVIDENCE_TODO.md and docs/screenshots/ to report screenshot status,
and scans docs/*.md for missing image files and unresolved TBD markers.
"""

import os
import re
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_DIR = os.path.join(PROJECT_ROOT, "docs")
SCREENSHOTS_DIR = os.path.join(DOCS_DIR, "screenshots")
EVIDENCE_TODO_FILE = os.path.join(DOCS_DIR, "EVIDENCE_TODO.md")


def audit_evidence():
    print("=" * 80)
    print("EVIDENCE & SCREENSHOT AUDIT REPORT")
    print("=" * 80)

    if not os.path.exists(EVIDENCE_TODO_FILE):
        print(f"[ERROR] Evidence checklist not found: {EVIDENCE_TODO_FILE}")
        sys.exit(1)

    # 1. Parse EVIDENCE_TODO.md
    with open(EVIDENCE_TODO_FILE, "r", encoding="utf-8") as f:
        todo_content = f.read()

    evidence_items = []
    # Table rows: | ID | Rubric | ... | Filename | Doc | Status |
    row_pattern = re.compile(r"^\|\s*(E\d+)\s*\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*([^|]+)\|\s*\[([ xX])\]\s*\|", re.MULTILINE)

    for match in row_pattern.finditer(todo_content):
        eid = match.group(1).strip()
        rubric = match.group(2).strip()
        filename = match.group(7).strip()
        target_doc = match.group(8).strip()
        is_checked = match.group(9).strip().lower() == "x"
        evidence_items.append({
            "id": eid,
            "rubric": rubric,
            "filename": filename,
            "doc": target_doc,
            "checked": is_checked
        })

    # Check files in docs/screenshots/
    existing_files = set(os.listdir(SCREENSHOTS_DIR)) if os.path.exists(SCREENSHOTS_DIR) else set()
    existing_files.discard(".gitkeep")

    present_count = 0
    missing_count = 0

    print(f"{'ID':<6} | {'Target Filename':<36} | {'Target Document':<30} | {'Status'}")
    print("-" * 85)

    for item in evidence_items:
        fname = item["filename"]
        exists = fname in existing_files
        if exists:
            present_count += 1
            status_str = "[OK] PRESENT"
        else:
            missing_count += 1
            status_str = "[ ] MISSING"
        print(f"{item['id']:<6} | {fname:<36} | {item['doc']:<30} | {status_str}")

    print("-" * 85)
    total_items = len(evidence_items)
    print(f"Total Evidence Items : {total_items}")
    print(f"Present Screenshots  : {present_count} / {total_items}")
    print(f"Missing Screenshots  : {missing_count} / {total_items}")
    print("=" * 80)

    # 2. Scan docs/*.md for broken image links and TBDs
    print("\nSCANNING DOCS FOR BROKEN IMAGE REFERENCES AND TBD MARKERS:")
    print("-" * 80)
    image_pattern = re.compile(r"!\[(.*?)\]\((.*?)\)")

    broken_links = 0
    tbd_count = 0

    for fname in sorted(os.listdir(DOCS_DIR)):
        if fname.endswith(".md") and fname != "EVIDENCE_TODO.md":
            fpath = os.path.join(DOCS_DIR, fname)
            with open(fpath, "r", encoding="utf-8") as f:
                lines = f.readlines()

            for idx, line in enumerate(lines, 1):
                # Check images
                for match in image_pattern.finditer(line):
                    img_path = match.group(2).strip()
                    if "screenshots/" in img_path:
                        target_name = os.path.basename(img_path)
                        if target_name not in existing_files:
                            broken_links += 1
                            # Image not yet captured, expected for student todo

                # Check TBD
                if "TBD" in line:
                    tbd_count += 1
                    clean_line = line.strip()
                    if len(clean_line) > 80:
                        clean_line = clean_line[:77] + "..."
                    print(f"  {fname}:{idx:<3} -> {clean_line}")

    print("-" * 80)
    print(f"Pending Screenshot Links (Awaiting capture): {broken_links}")
    print(f"Unresolved TBD Markers in Documentation   : {tbd_count}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    audit_evidence()
