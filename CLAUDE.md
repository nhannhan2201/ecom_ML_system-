@AGENTS.md

## Claude Code Specific Instructions

- **Planning First**: Always review `RUBRIC.md` and outline an explicit multi-step plan before making changes across multiple modules.
- **Verification Loop**: After modifying Python code or configuration, immediately execute `make lint` and `make test`. Ensure all 22 tests pass and coverage is maintained.
- **Evidence Integrity**: When updating technical reports in `docs/`, only record verified output numbers from `docs/evidence/` or actual terminal runs; use `TBD` for unexecuted benchmarks.
- **Handoff Discipline**: Commit every distinct unit of work with clear, conventional messages matching the active rubric task.
