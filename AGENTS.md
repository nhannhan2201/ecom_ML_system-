# Instructions for Codex agents

This file explains how to use and maintain the project Markdown files. The learning and pair-programming method lives in [LEARNING_ROADMAP.md](LEARNING_ROADMAP.md); follow it when guiding the learner.

## Read order at the start of a task

1. Read this file and root [INDEX.md](INDEX.md).
2. Read the current milestone and latest recorded verification in [LEARNING_ROADMAP.md](LEARNING_ROADMAP.md).
3. Consult [WorkFlow.md](WorkFlow.md) for agreed problem/design decisions, [DATA_FLOW.md](DATA_FLOW.md) for the source graph, and [RUBRIC.md](RUBRIC.md) for the criteria relevant to the task.
4. Read [docs/INDEX.md](docs/INDEX.md) only when the task needs component-specific technical notes or evidence. Then read only the relevant note.

## Keep each file in its lane

- `LEARNING_ROADMAP.md`: learning sequence, current milestone, what was understood/changed, commands actually run and their actual results, remaining questions, next step. Update after every completed learning milestone or when the chosen milestone/status changes. Do not record unrun checks as passed.
- `WorkFlow.md`: agreed product/data problem, target tables and target flow. Change it only when a design decision changes; distinguish decisions from open questions. Never use it to claim code/runtime success.
- `DATA_FLOW.md`: current static connections in source, entrypoints, outputs/consumers and known gaps. Update after inspecting code changes or completing a relevant source audit. Label source-confirmed links separately from runtime-verified behavior.
- `RUBRIC.md`: workbook requirements and per-criterion implementation/evidence status. Update only the criteria touched by a milestone; include a real evidence reference before marking verified. Tests alone do not prove pipeline runtime or scale requirements.
- `docs/`: technology/component explanations and fresh runtime evidence. Create or update a focused note when there is useful verified technical material; record version, code/files, command/input, date, result and limits. Link it from `docs/INDEX.md`. Do not copy the whole roadmap or target architecture into a component note.
- `INDEX.md`: short navigation to root project guidance. `docs/INDEX.md`: index of component notes/evidence only.

After each run, update the Roadmap with the exact command, exit/result, scope and limitations. If the run creates useful rubric evidence, add/link it under `docs/` and update the matching Rubric row. If a source relationship changed, update `DATA_FLOW.md`. If the agreed design changed, update `WorkFlow.md`. Keep changes limited to the affected files and preserve unrelated user edits.

## Operational safeguards

- Teach in Vietnamese; never read or print `.env`, credentials, tokens or secrets. Do not commit for the learner.
- Work on one component/data slice per milestone; skip Novel Ideas. Check imports and references from Makefile, Compose, DAGs, scripts, tests and docs before removing project files.
- Do not reset, delete volumes/topics/checkpoints/data, overwrite outputs, run large generation, or incur cloud cost without explaining impact and receiving the learner's agreement.
- Before running a command, identify whether it writes or changes external/local state. Prefer fixtures and isolated destinations.
- Do not invent benchmark numbers, runtime status, or official-doc claims. Verify library versions from requirements/config/image before consulting matching official docs.
