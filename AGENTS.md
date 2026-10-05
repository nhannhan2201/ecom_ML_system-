# Instructions for Codex and other project agents

This file explains how to maintain the project's documentation and verified progress. Follow the root [INDEX.md](INDEX.md) reading order and use [IMPLEMENTATION_ROADMAP.md](IMPLEMENTATION_ROADMAP.md) for the beginner-friendly, one-component-at-a-time learning process.

## Documentation ownership

- [TARGET_ARCHITECTURE.md](TARGET_ARCHITECTURE.md) is the agreed product/data design. Update it only when a design decision changes; do not use it to claim implementation or runtime success.
- [DATA_CONTRACT.md](DATA_CONTRACT.md) is the canonical meaning/schema/grain/key/timestamp contract. It distinguishes CURRENT source declarations from TARGET requirements. Update it when inspection verifies a schema change or the learner approves a contract decision.
- [CURRENT_IMPLEMENTATION.md](CURRENT_IMPLEMENTATION.md) maps current source/config connections and gaps. Update it after source audits or implementation changes; mark links as static source evidence unless runtime was actually verified.
- [RUBRIC.md](RUBRIC.md) tracks workbook criteria and implementation/evidence status. Change only relevant criteria, and link actual evidence before marking a runtime requirement verified.
- [IMPLEMENTATION_ROADMAP.md](IMPLEMENTATION_ROADMAP.md) keeps the learning order, current milestone, what was understood/changed, exact checks and results, docs updated, and unresolved questions. It is the progress log; do not create a report for every small code edit.
- `docs/` contains reusable technology explanations and fresh evidence. Add/update a component note only when it adds useful technical understanding or records a real run; include version, code, command/input, time, result and limitations, then link it from [docs/INDEX.md](docs/INDEX.md).
- [README.md](README.md) is the short project entry point. [INDEX.md](INDEX.md) is the root reading order; [docs/INDEX.md](docs/INDEX.md) indexes component notes/evidence.

## Pair-programming workflow

Use functional component names in filenames, docs and communication (for example, Batch Generator or Kafka Stream Replay); never refer to work only by a milestone code. If order is needed, append the roadmap number to the component name. Map names from the roadmap; ask the learner when the mapping is unclear. Historical commands/removed paths and fixture data keep their original literal values, explicitly identified as historical or test data.

Learning flow: Understand purpose → Input / Output → Native tech-stack concept → Minimal implementation → Tiny test → Small real-data runtime → Evidence → Learner can explain the flow → DONE. Passing tests alone does not mark a component DONE. Before code for a new component, explain its role, input/output, proposed files and native technology concepts, then STOP for learner approval. Do not automatically start the next component. Legacy code in `old-vibe-backup` is reference only; validate against contract/rubric before reuse.

Teach in Vietnamese for a beginner. For one component or data slice per milestone: explain its purpose, problem, inputs/outputs and important files first; then explain its connections. Draw Mermaid from inspected code and distinguish source-declared links from runtime-verified behavior. Check dependency versions in project config before making version-specific claims. Trace input → schema → processing → output → consumer. Compare with relevant rubric rows, tests and traceable evidence; classify findings as verified, implemented but unverified, different from docs, or needing investigation.

Give the learner one short chance to predict an input/output or explain a code section before revealing the answer. Before editing, explain a small example, the problem and invariant. Make one scoped change only. Afterwards walk the diff: purpose, callers/callees, input/output and debugging path. Run appropriate checks and record exact commands/results. Finish the milestone with paper-ready notes in the roadmap: purpose, input/output, diagram, key files, commands/checks, common failures, what was learned, changes/reasons and remaining uncertainty.

## Evidence and safe operations

- Preserve existing user changes. Inspect `git status` and diff before work; never commit for the learner.
- Never read or print `.env`, credentials, tokens or secrets. Do not invent measurements, runtime status or official documentation claims.
- Unit tests do not prove pipeline runtime or scale. Label static source inspection, unit verification, runtime readback and benchmark evidence separately.
- Read only files needed for the active milestone. Before cleanup, check Python imports, Makefile, Compose, DAGs, scripts, tests and docs references; present cleanup candidates and reasons before removal.
- Before commands, explain whether they write local/external state. Prefer small fixtures and isolated destinations. Never reset state, delete volumes/topics/checkpoints/data, overwrite datasets, run large generation or incur cloud costs without explaining the impact and receiving agreement.
- Do not run unrelated tests for a documentation-only task. For code changes, use the repository's relevant checks and record any baseline failure accurately.

## Updating docs after a run or code change

Update only the owning references: implementation changes can affect `CURRENT_IMPLEMENTATION.md` and `DATA_CONTRACT.md`; design decisions affect `TARGET_ARCHITECTURE.md`; a completed learning milestone updates `IMPLEMENTATION_ROADMAP.md`; newly satisfied rubric criteria update `RUBRIC.md`; reusable component explanations/evidence go in `docs/`. Then check links and `git diff --check`. Never mark a claim verified merely because the command was launched; require its actual result and, for external systems, a suitable readback.
