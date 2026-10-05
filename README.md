# ecom_ML_system

An e-commerce event data-engineering coursework project using REES46 data, Kafka, Flink, Spark, a MinIO/Delta Lakehouse, PostgreSQL, Feast/Redis, Airflow and DataHub. The repository contains code for parts of this system; source inspection alone does not establish that a pipeline ran successfully. Novel Ideas rubric items are out of scope for the current learning plan.

The agreed model baseline predicts whether a user will purchase in the hour after a decision time using four event-time features over the preceding 15 minutes. The old 30-day batch feature path and candidate-minute label code were removed from the rebuild baseline. Read the current-versus-target distinction before treating these paths as equivalent.

## Start here

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
| `src/generator/` | Retained M1 batch generator |
| `tests/` | Unit, property and structure tests |
| `docs/` | Component explanations and new evidence after verification |

`Makefile` and old pipeline jobs were removed. Run M1 with `python -m pytest tests/test_batch_selection_m1.py -q`; tests verify code-level properties only; they do not prove that external services or data pipelines completed successfully. The retained generator can write local or external state. Use the roadmap's scoped milestone and isolated destinations before running them. Do not commit automatically.
