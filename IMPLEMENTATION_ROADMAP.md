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

- **Current milestone:** M1 selection/sampling verified at unit and small local runtime levels on 2026-10-05; stopped for learner review. Do not start M2 automatically.
- **Design:** four canonical 15-minute features predict at least one purchase during the following hour. See [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md).
- **Contract:** the root [DATA_CONTRACT.md](DATA_CONTRACT.md) now records current source declarations and the target schema separately.
- **Source audit:** root docs reorganized and current generator/Spark/Flink/Feast/DWH paths inspected. This was static inspection only; no pipeline/test was run in this documentation task.
- **Historical runtime progress (artifacts removed during rebuild cleanup):** [M1 small local batch readback](docs/m1_batch_selection.md): 1,000 selected source rows, 1,020 rows after existing transformation; all output timestamps/schema memberships pass. No MinIO/downstream/full-output verification.
- **Unresolved:** November coverage/overlap and source-order audit (classification no longer assumes ordering); event identity and dedup rule; prediction eligibility/snapshot availability; watermark and late-data policy; Feast/DWH physical readback.

## Milestone log

M1 record is below. For later milestones, use the same concise record format:

- **What I can now explain:** component purpose and its place in the full data flow.
- **Change and reason:** exact scoped code/doc change, or “no code change”.
- **Verification:** exact command and real exit/output summary; specify unit, runtime readback or benchmark.
- **Docs updated:** links to changed source-of-truth/component notes.
- **Still uncertain:** questions that require a later fixture, source inspection or runtime check.


### M1 — October batch selection and schema boundary (2026-10-05)

- **Purpose / what I can now explain:** read → parse/validate UTC event_time → classify → independently sample OLD/NEW → existing transformation. October is batch; November is the stream target. OLD `[Oct 01,Oct 16)`, NEW `[Oct 16,Nov 01)`. INVALID and EXCLUDED belong to neither. Source order does not change membership; it may change sampled identities.
- **Diagram:** [M1 component flow](docs/m1_batch_selection.md). Inputs are nine-column REES46 CSV events; outputs are selected source rows, classification counts, then local OLD nine-column / NEW ten-column CSV.
- **Inspected/changed:** `src/generator/batch_generator.py`, `config/generator_config.yaml`, `tests/test_batch_selection_m1.py`, `tests/fixtures/m1_october_boundaries.csv.fixture`; inspected relevant existing tests and Makefile/rehearsal calls. Previously added `tests/m1_readback.py`; that script was removed during rebuild cleanup.
- **Change and reason:** remove row-offset/schema coupling and independent hard-coded dates; validate sole UTC config; random-priority reservoir sampling per population. Shortages return available rows without quota transfer/duplication. Add isolated local destination, counts in manifest. Benchmark retains scale/replica semantics and uses shared classifier; no benchmark run.
- **Exact checks/results:** `python -m pytest tests/test_batch_selection_m1.py -q -s` → 9 passed, 12/12 fixture classifications match (OLD=4, NEW=5, EXCLUDED=2, INVALID=1); reordered-source membership, deterministic sampling, chunk-size sampling, shortage, empty population and invalid-input checks pass. Final renamed-fixture run `python -m pytest tests/test_batch_selection_m1.py -q` → 9 passed. `python -m pytest tests/test_generator.py -q -k 'schema_evolution_part or seed_sequence_reproducibility'` → 3 passed, 15 deselected.
- **Small runtime:** `python src/generator/batch_generator.py --mode small --sample-size 1000 --local-output-dir /tmp/ecom-m1-runtime-kWrAz2` → exit 0. Source counts OLD=20,442,805; NEW=22,005,959; INVALID=0; EXCLUDED=0. Selected 500+500, output 510+510 after existing transformation. `python tests/m1_readback.py /tmp/ecom-m1-runtime-kWrAz2` → exit 0; independent parse confirms bounds and exact schemas, no wrong membership. Historical result only: evidence JSON/manifest/log and the readback script were removed during cleanup; no fresh runtime run is claimed.
- **Common failures/debug:** invalid config order or UTC notation raises; malformed event_time increments INVALID; CSV structural errors raise; insufficient schema population logs shortage. Inspect classification before sampling, then manifest counts, then read back output independently. Small output still requires a full source scan.
- **Docs updated:** target design, data contract, current implementation, this roadmap, [component evidence](docs/m1_batch_selection.md) and docs index. Rubric not marked complete: M1 does not verify all generator requirements.
- **Still uncertain / stop:** full October output, medium/full scale and replication, duplicate/skew/distribution correctness, MinIO delivery, November stream and downstream systems are NOT YET VERIFIED. No M2 work; awaiting learner review.


### Final rebuild cleanup — 2026-10-05

- **Scope:** cleanup đã được learner approve; giữ M1 generator/config/tests/fixture, source CSV, workbook, requirements và tài liệu đã chốt. Không sửa logic M1 hoặc bắt đầu M2.
- **Artifact references:** evidence JSON/log và readback script cũ đã dọn; ghi chú M1 giữ lịch sử, không chứng nhận một runtime mới. `CURRENT_IMPLEMENTATION.md` chỉ mô tả source còn trong baseline.
- **Checks:** `git diff --check` → exit 0; `PYTHONDONTWRITEBYTECODE=1 python -m pytest tests/test_batch_selection_m1.py -q -p no:cacheprovider` → 9 passed in 1.22s (final rerun after learner removed logs). Local Markdown link check → 0 broken links. Protected source/config/tests/fixture/workbook/requirements, DATA_CONTRACT, TARGET_ARCHITECTURE và RUBRIC byte-identical với HEAD. Hai CSV còn trên disk, không tracked và được ignore.
- **Cleanup completed:** learner removed the permission-blocked Airflow logs; final inspection confirms `logs/` and every approved artifact are absent. The earlier temporary copy was removed. No cleanup blocker remains. Baseline commit authorized with message `chore: establish clean rebuild baseline`; no push, no M2.
