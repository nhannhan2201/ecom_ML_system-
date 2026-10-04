# Implementation roadmap and learning progress

This is the learning sequence and progress log: it records what to study next and what has actually been understood, changed and verified. It is not the canonical schema document ([DATA_CONTRACT.md](DATA_CONTRACT.md)), target design ([TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md)), current source map ([CURRENT_IMPLEMENTATION.md](CURRENT_IMPLEMENTATION.md)), or rubric checklist ([RUBRIC.md](RUBRIC.md)).

## How to learn one component

For every milestone, first explain the component's purpose, problem, inputs/outputs and relevant files. Then show its real connections in Mermaid and separate source-declared edges from runtime-verified behavior. Confirm library versions from project config before version-specific explanations; trace only the current slice input → schema → processing → output → consumer. Compare source with the contract, rubric, tests and traceable evidence. Give the learner one short chance to predict output or explain a code section before revealing the answer.

Before code changes, explain a small example, the specific problem and invariant. Make one small, scoped change, then review each diff section together (purpose, caller/callee, input/output, debugging approach). Run appropriate checks and record exact command and actual result. Unit tests do not establish a pipeline run. End each milestone with paper-ready notes: purpose, input/output, diagram, important files, commands/checks, common failures, what was learned, change/reason and uncertainty. Update this file rather than creating a separate report for every small edit.

Use Vietnamese and beginner-friendly language. Skip Novel Ideas. Preserve user changes. Do not read secrets or commit. Explain side effects before running commands; do not reset, delete volumes/topics/checkpoints/data, overwrite outputs, run large generation or incur cloud cost without an explicit agreement. Before cleanup, inspect imports, Makefile, Compose, DAGs, scripts, tests and docs references.

## Milestones

| # | Milestone | Exit criteria |
| --- | --- | --- |
| 1 | Batch generator: choose rows and date boundaries | A tiny fixture proves intended rows, half-open boundaries and no overlap/loss. |
| 2 | Batch generator: schema and deterministic fields | Old/new schema evolution and effective config are explicit; seed behavior is repeatable. |
| 3 | Batch duplicates/cardinality/skew | Identity, duplicate denominator, skew and replica effects are understood and fixture-checked. |
| 4 | Batch output → MinIO raw | Isolated output can be read back and reconciled with source/manifest. |
| 5 | Stream replay → Kafka | Distinguish event time, production, callback/ack, key and checkpoint; bounded topic readback. |
| 6 | Late-event buffer/restart | Determine event release order and loss/duplication conditions after interruption. |
| 7 | Spark DP1: raw → Bronze | Bootstrap/staging/archive behavior, schema and retries reconcile in an isolated Delta snapshot. |
| 8 | Spark DP2: Bronze → Silver | Cast/null/dedup behavior is explainable; every removed row has a reason. |
| 9 | Flink event time and canonical 15m features | Watermark, dedup and hopping window pass an on-time/late event fixture with hand-calculated output. |
| 10 | Spark Silver → Gold and DWH | SCD2 grain/interval, temporal join, fact key and PostgreSQL mapping reconcile. |
| 11 | Airflow DP1–DP3 | Every dependency edge is explained; task success, DAG completion and output readback are distinct. |
| 12 | Offline feature history and labels | Spark output follows the agreed 15m feature/prediction-linked label contract without leakage. |
| 13 | Feast historical retrieval/training | Point-in-time joins use exact schema/timestamps; missing rows are not silently mistaken for zero. |
| 14 | Incremental materialization | Isolated offline-to-online run/readback verifies cursor, freshness and stale overwrite behavior. |
| 15 | Stream feature pusher | Kafka offsets, retry/flush and offline/online writes reconcile in an isolated test. |
| 16 | Governance and DataHub | One dataset's contract/lineage is checked against actual schema and catalog readback. |
| 17 | Storage optimization | One optimization has before/after correctness checks and attributable evidence. |
| 18 | Docker/resources | One service/image's versions, dependencies and resource behavior are inspected and verified. |
| 19 | Training/API/remaining rubric | Inventory actual code first; split work into narrow rubric-aligned milestones. |
| 20 | Scale and benchmarking | Only after local correctness; isolated destinations, capacity constraints and repeatable measurement plan. |

## Current status

- **Current milestone:** M1 — batch generator row selection and date boundaries. Start with explanation and a tiny fixture design; do not run a large generator.
- **Design:** four canonical 15-minute features predict at least one purchase during the following hour. See [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md).
- **Contract:** the root [DATA_CONTRACT.md](DATA_CONTRACT.md) now records current source declarations and the target schema separately.
- **Source audit:** root docs reorganized and current generator/Spark/Flink/Feast/DWH paths inspected. This was static inspection only; no pipeline/test was run in this documentation task.
- **Verified runtime progress:** no new runtime verification recorded for this documentation task.
- **Unresolved:** source CSV overlap/order/coverage; event identity and dedup rule; prediction eligibility/snapshot availability; watermark and late-data policy; Feast/DWH physical readback.

## Milestone log

No code-learning milestone has been completed in the current roadmap. For the next completed milestone, add one concise entry here with:

- **What I can now explain:** component purpose and its place in the full data flow.
- **Change and reason:** exact scoped code/doc change, or “no code change”.
- **Verification:** exact command and real exit/output summary; specify unit, runtime readback or benchmark.
- **Docs updated:** links to changed source-of-truth/component notes.
- **Still uncertain:** questions that require a later fixture, source inspection or runtime check.
