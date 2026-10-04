#!/usr/bin/env python3
"""
================================================================================
SCRIPT: DWH EXPLAIN ANALYZE BENCHMARK
Project: E-Commerce Real-Time Purchase Propensity Prediction System
Purpose:
  Benchmarks PostgreSQL DWH query performance on gold.fact_user_events (1.4M rows)
  comparing Sequential Scan (Baseline) vs Composite B-Tree Index (Optimized).
  Saves real execution plan outputs to docs/evidence/dwh_explain_analyze.txt.
================================================================================
"""

import os
import psycopg2


def run_benchmark():
    host = os.environ.get("POSTGRES_DWH_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_DWH_PORT", 5432))
    dbname = os.environ.get("POSTGRES_DWH_DB", "ecom_dwh")
    user = os.environ.get("POSTGRES_DWH_USER", "postgres")
    password = os.environ.get("POSTGRES_DWH_PASSWORD", "postgres")

    print(f"Connecting to PostgreSQL DWH at {host}:{port}/{dbname}...")
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    cur = conn.cursor()

    # Query typical for real-time feature serving / recent user activity retrieval
    test_query = """
        SELECT event_type, product_sk, price, event_time 
        FROM gold.fact_user_events 
        WHERE user_id = 513359812 AND event_time >= '2019-10-16 04:15:13'
        ORDER BY event_time DESC;
    """

    output_lines = [
        "================================================================================",
        "POSTGRESQL DWH EXPLAIN (ANALYZE, BUFFERS) BENCHMARK REPORT",
        "Table: gold.fact_user_events (~1.4M rows)",
        "================================================================================",
        "",
        "--- QUERY BEING TESTED ---",
        test_query.strip(),
        "",
        "--- 1. BASELINE EXECUTION (INDEX SCAN DISABLED: FORCED SEQUENTIAL SCAN) ---",
    ]

    # 1. Force Seq Scan
    cur.execute("SET enable_indexscan = off;")
    cur.execute("SET enable_bitmapscan = off;")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {test_query}")
    seq_plan = cur.fetchall()
    for row in seq_plan:
        output_lines.append(row[0])

    output_lines.extend(
        ["", "--- 2. OPTIMIZED EXECUTION (COMPOSITE B-TREE INDEX ENABLED: idx_fact_user_events_user_time) ---"]
    )

    # 2. Enable Index Scan
    cur.execute("SET enable_indexscan = on;")
    cur.execute("SET enable_bitmapscan = on;")
    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) {test_query}")
    idx_plan = cur.fetchall()
    for row in idx_plan:
        output_lines.append(row[0])

    output_lines.append("================================================================================")

    result_text = "\n".join(output_lines)
    print(result_text)

    os.makedirs("docs/evidence", exist_ok=True)
    out_file = "docs/evidence/dwh_explain_analyze.txt"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(result_text + "\n")
    print(f"\nSaved benchmark evidence to {out_file}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    run_benchmark()
