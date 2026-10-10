# ecom_ML_system

An e-commerce event data-engineering coursework project using REES46 data, Kafka, Flink, Spark, a MinIO/Delta Lakehouse, PostgreSQL, Feast/Redis, Airflow and DataHub. The repository contains code for parts of this system; source inspection alone does not establish that a pipeline ran successfully. Novel Ideas rubric items remain optional/bonus backlog in the current learning plan.

The agreed model baseline predicts whether a user will purchase in the hour after the feature snapshot time using four event-time features over the preceding 15 minutes. The old 30-day batch feature path and candidate-minute label code were removed from the rebuild baseline. Read the current-versus-target distinction before treating these paths as equivalent.

## Start here

Session mới: [handoff ngắn](docs/SESSION_HANDOFF.md). Target E2E giữ October Medallion → Features/Labels → Kubeflow/MLflow → KServe; November timeline 1× → Kafka behavior → Flink → offline history + Feast-compatible Redis → FastAPI → KServe. Không feature topic/consumer riêng; direct-write recovery còn OPEN.

Read the [project documentation map](INDEX.md) in order. The central references are:

- [Rubric requirements and evidence status](RUBRIC.md)
- [Target architecture](TARGET_ARCHITECTURE.md)
- [Canonical data contract](DATA_CONTRACT.md)
- [Current source implementation map](CURRENT_IMPLEMENTATION.md)
- [Learning and implementation progress](IMPLEMENTATION_ROADMAP.md)
- [Component documentation and evidence index](docs/INDEX.md)

## Repository layout

| Path | Role |
| --- | --- |
| `rubic/` | Coursework workbooks |
| `config/` | Generator and system configuration |
| `src/generator/` | Batch Generator and Kafka Stream Replay |
| `src/spark/` | Raw → Bronze Delta job (smoke/full) |
| `tests/` | Unit, property and structure tests |
| `docs/` | Component explanations and new evidence after verification |

Historical [Batch Generator → MinIO small runtime](IMPLEMENTATION_ROADMAP.md#batch-generator--minio-raw-storage--2026-10-06) is verified (2026-10-06): healthy MinIO, three objects and CSV readback; ≥100 GB and downstream remain unverified. `compose.yaml` declares MinIO and Kafka. CLI loads the root `.env` without overriding existing environment variables.

`Makefile` and old pipeline jobs were removed. Run Batch Generator unit tests with `python -m pytest tests/test_batch_generator.py -q`; tests verify code-level properties only; they do not prove that external services or data pipelines completed successfully. The retained generator can write local or external state. Use the roadmap's scoped milestone and isolated destinations before running them. Do not commit automatically.

[Full October Batch Generator evidence](docs/evidence/batch_generator_october.json): learner native readback PASS for 42,448,764 source + 848,975 injected copies, date-based schema evolution and bytes/hashes. Two CSVs in MinIO, only small local reports; natural duplicates/skew/≥100 GB remain unverified. [Current commands](docs/batch_generator.md).

Current status: [full October Spark Raw → Bronze Delta Schema Evolution PASS](docs/evidence/spark_raw_to_bronze_evolution_full.json), 43,297,739 rows with duplicates retained; OLD creates Delta v0 (13 columns), NEW appends with `mergeSchema=true` to v1 (14 columns). [Runtime command, explanation and limits](docs/spark_raw_to_bronze.md). Silver, features/labels, Airflow and optimization are not implemented/verified by this run.
