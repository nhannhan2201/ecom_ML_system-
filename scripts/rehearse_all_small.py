"""Automated End-to-End Rehearsal Pipeline (Small Mode).

Executes provisioning and processing commands for the existing small-mode pipeline:
1. Check services health
2. Initialize Airflow connections & DWH schemas
3. Generate batch small dataset to MinIO
4. Execute Spark Bronze -> Silver -> Gold transformation
5. Trigger Airflow DAGs without waiting for their final runtime status
6. Synchronize DataHub catalog and verify contracts
7. Apply Feast and materialize batch features

Logs detailed timing and PASS/FAIL status for each step to docs/evidence/rehearsal_log.txt.
Stops on a nonzero command exit; this does not prove end-to-end data correctness.
"""

import os
import subprocess
import sys
import time
from datetime import datetime, timezone


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVIDENCE_DIR = os.path.join(PROJECT_ROOT, "docs", "evidence")
LOG_FILE = os.path.join(EVIDENCE_DIR, "rehearsal_log.txt")


STEPS = [
    {
        "name": "Check Services Health",
        "cmd": [sys.executable, "scripts/check_services.py"],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Initialize MinIO Buckets",
        "cmd": [sys.executable, "scripts/init_minio_buckets.py"],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Initialize DWH Schemas",
        "cmd": [sys.executable, "scripts/setup_dwh_schemas.py", "--schema-only"],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Initialize Airflow Connections",
        "cmd": [
            "docker",
            "exec",
            "airflow_webserver",
            "python",
            "/opt/airflow/ecom_project/scripts/init_airflow_connections.py",
        ],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Generate Small Batch Data",
        "cmd": [sys.executable, "src/generator/batch_generator.py", "--mode", "small"],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Spark DP1: Bronze Lakehouse Ingestion",
        "cmd": [
            sys.executable,
            "src/spark/spark_optimized.py",
            "--stage",
            "dp1",
            "--step",
            "all",
            "--no-wait",
        ],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Spark DP2: Silver & Gold DWH Ingestion",
        "cmd": [
            sys.executable,
            "src/spark/spark_optimized.py",
            "--stage",
            "dp2",
            "--step",
            "all",
            "--no-wait",
        ],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Spark DP3: Features & Labels Computation",
        "cmd": [
            sys.executable,
            "src/spark/spark_optimized.py",
            "--stage",
            "dp3",
            "--step",
            "all",
            "--no-wait",
        ],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Sync DWH from Lakehouse & Benchmark Indexing",
        "cmd": [sys.executable, "scripts/setup_dwh_schemas.py"],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Trigger Airflow DP1 Pipeline",
        "cmd": ["docker", "exec", "airflow_scheduler", "airflow", "dags", "trigger", "dp1_raw_to_bronze"],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "Trigger Airflow DP4 Feast Materialize",
        "cmd": ["docker", "exec", "airflow_scheduler", "airflow", "dags", "trigger", "dp4_feast_materialize"],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
    {
        "name": "DataHub Metadata Sync",
        "cmd": [sys.executable, "governance/sync_catalog.py"],
        "cwd": PROJECT_ROOT,
        "env": {**os.environ.copy(), "PYTHONPATH": f"{PROJECT_ROOT}/governance:{os.environ.get('PYTHONPATH', '')}"},
    },
    {
        "name": "DataHub Contract Verification",
        "cmd": [sys.executable, "governance/verify_contracts.py"],
        "cwd": PROJECT_ROOT,
        "env": {**os.environ.copy(), "PYTHONPATH": f"{PROJECT_ROOT}/governance:{os.environ.get('PYTHONPATH', '')}"},
    },
    {
        "name": "Feast Apply Definitions",
        "cmd": ["feast", "apply"],
        "cwd": os.path.join(PROJECT_ROOT, "feature_store"),
        "env": os.environ.copy(),
    },
    {
        "name": "Feast Materialize",
        "cmd": [sys.executable, "feature_store/materialize.py", "--mode", "incremental"],
        "cwd": PROJECT_ROOT,
        "env": os.environ.copy(),
    },
]


def run_rehearsal():
    """Run the configured pipeline commands and record their exit status and timing."""
    os.makedirs(EVIDENCE_DIR, exist_ok=True)
    start_all = time.time()
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    log_lines = [
        "=" * 80,
        "END-TO-END PIPELINE REHEARSAL LOG (SMALL MODE)",
        "=" * 80,
        f"Execution Timestamp : {now_iso}",
        "Target Mode         : small (sample size from generator configuration)",
        f"Repository Root     : {PROJECT_ROOT}",
        "=" * 80,
        f"{'Step Name':<42} | {'Duration':<10} | {'Status'}",
        "-" * 80,
    ]

    print("=" * 80)
    print("STARTING END-TO-END REHEARSAL (SMALL MODE)")
    print("=" * 80)

    for idx, step in enumerate(STEPS, 1):
        step_name = step["name"]
        cmd = step["cmd"]
        cwd = step["cwd"]
        env = step["env"]

        cmd_str = " ".join(cmd)
        print(f"\n[{idx}/{len(STEPS)}] Running: {step_name} ...")
        print(f"       Command: {cmd_str}")

        t0 = time.time()
        try:
            proc = subprocess.run(
                cmd,
                cwd=cwd,
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            duration = time.time() - t0
            passed = proc.returncode == 0

            status_str = "PASS" if passed else "FAIL"
            log_lines.append(f"{step_name:<42} | {duration:>7.2f}s   | [{status_str}]")

            if passed:
                print(f"       Result: PASS ({duration:.2f}s)")
            else:
                print(f"       Result: FAIL ({duration:.2f}s, exit code {proc.returncode})")
                print("\n" + "=" * 80)
                print(f"ERROR DETAILS FOR STEP: {step_name}")
                print("=" * 80)
                if proc.stderr:
                    print(proc.stderr.strip()[-3000:])
                if proc.stdout:
                    print(proc.stdout.strip()[-3000:])
                print("=" * 80)

                log_lines.extend([
                    "-" * 80,
                    f"REHEARSAL ABORTED AT STEP: {step_name}",
                    f"Exit code: {proc.returncode}",
                    f"Error output snippet:\n{proc.stderr.strip()[-1500:]}",
                    "=" * 80,
                ])
                with open(LOG_FILE, "w", encoding="utf-8") as f:
                    f.write("\n".join(log_lines) + "\n")
                sys.exit(1)

        except Exception as e:
            duration = time.time() - t0
            print(f"       Result: EXCEPTION ({e})")
            log_lines.append(f"{step_name:<42} | {duration:>7.2f}s   | [EXCEPTION: {e}]")
            with open(LOG_FILE, "w", encoding="utf-8") as f:
                f.write("\n".join(log_lines) + "\n")
            sys.exit(1)

    total_time = time.time() - start_all
    log_lines.extend([
        "-" * 80,
        f"TOTAL REHEARSAL DURATION: {total_time:.2f}s",
        f"OVERALL REHEARSAL RESULT: ALL {len(STEPS)} STEPS PASSED SUCCESSFULLY",
        "=" * 80,
    ])

    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(log_lines) + "\n")

    print("\n" + "=" * 80)
    print(f"REHEARSAL COMPLETED SUCCESSFULLY in {total_time:.2f}s!")
    print(f"Rehearsal log written to: {LOG_FILE}")
    print("=" * 80)


if __name__ == "__main__":
    run_rehearsal()
