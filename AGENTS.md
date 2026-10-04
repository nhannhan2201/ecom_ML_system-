# Agent Handoff Guide - ecom_ML_system

## Project Overview
An end-to-end e-commerce real-time & batch data engineering platform (EDAI K11 DE Mini-Coursework) ingesting REES46 events, processing via Spark & Flink, storing across a MinIO Delta Lakehouse and PostgreSQL DWH, serving features via Feast/Redis, and governed by DataHub & Airflow.

## Technology Stack
- **Engine**: Apache Spark 3.5.0 (Delta Lake 3.0.0), Apache Flink 1.17.1 (PyFlink)
- **Storage**: MinIO S3 (Bronze/Silver/Gold Lakehouse), PostgreSQL 15 (Star Schema DWH), Redis 7 (Feast Online Store)
- **Messaging & Governance**: Apache Kafka 7.5.0, Apache Airflow 2.7.3, DataHub GMS 0.13, Feast 0.38

## Repository Layout
- `config/`: System and generator YAML configuration files.
- `dags/`: Airflow DAG pipelines (DP1: Ingest, DP2: Clean/DWH, DP3: Feat/Labels, DP4: Feast Materialize).
- `docker/`: Compose stacks for all infrastructure and optimized multistage Dockerfile.
- `docs/`: Technical reports and documentation mapped to rubric items (see `docs/INDEX.md`).
- `docs/evidence/`: Measured benchmark outputs (Docker size, DWH query plans, lakehouse files, data profiles).
- `feature_store/`: Feast definitions (`features.py`), configuration, materialization, and retrieval scripts.
- `governance/`: Declarative DataHub metadata catalog (`catalog.py`) and assertion verifier (`verify_contracts.py`).
- `notebooks/`: Verification and data exploration Jupyter notebooks.
- `scripts/`: Operational scripts (DWH schema setup, MinIO buckets, lakehouse inspector, profiling).
- `src/`: Core application source code (`generator/`, `spark/`, `flink/`, `api/`).
- `tests/`: Automated unit and DAG structure test suite (pytest, hypothesis).

## Core Commands (`Makefile`)
- `make help`: List all available automation targets.
- `make up-infra` / `make up-flink` / `make up-airflow` / `make up-datahub`: Spin up Docker compose stacks.
- `make gen-data` / `make gen-data-skewed`: Generate small batch dataset with optional skew injection.
- `make spark-opt`: Run optimized Spark Bronze-to-Silver & Gold Lakehouse transformation.
- `make flink-opt`: Submit optimized Flink streaming job (parallelism=3, hopping window).
- `make governance-verify`: Validate Lakehouse data contracts and publish assertions to DataHub.
- `make test` / `make lint`: Run automated pytest suite with coverage and ruff linter.

## Architectural Conventions
1. **Naming**: Strict prefixing: `raw_` (Bronze), `stg_` (Silver), `dim_` (Gold DWH Dim), `fact_` (Gold DWH Fact), `feat_` (Gold Feast Feature View).
2. **Feature Store Temporal Schema**: All `feat_` tables must provide `event_timestamp` and `created` columns.
3. **Secrets & Config**: Zero hardcoded credentials or machine paths (`/home/...`). All secrets via `.env` or Airflow Connections (`minio_s3_conn`, `postgres_dwh`, `redis_default`).
4. **Code Quality**: Every file requires a module header docstring; every class and function requires descriptive docstrings.
5. **Documentation**: Technical deliverables reside in `docs/` with explicit explanations of what each screenshot proves.

## Operational Rules for Agents
- **Zero Hallucination**: Never fabricate benchmark numbers or timings. Use `TBD (run <cmd> to measure)` if not executed.
- **Rubric-Driven**: Work on one rubric item per session and update status in [RUBRIC.md](RUBRIC.md).
- **Targeted Reading**: Consult `docs/INDEX.md` before reading documentation. Avoid scanning `docs/screenshots/` or `*.ipynb` unless strictly needed.
- **Verification**: Always run `make test` and `make lint` before concluding any code changes.
