# Implementation Plan - Refactor CLI

## Phase 1: Scaffolding and DB Command
- [x] Task: Create new directory structure `src/leakharvester/cli/commands` and `src/leakharvester/cli/__init__.py`. 3e62a55
    - [ ] Create `src/leakharvester/cli/commands/__init__.py`
    - [ ] Create `src/leakharvester/cli/main.py` as the new entry point.
- [x] Task: Refactor `db` command. b69be9b
    - [ ] Write failing tests for `db` command logic.
    - [ ] Move `db` command logic from `src/leakharvester/main.py` to `src/leakharvester/cli/commands/db.py`.
    - [ ] Verify functionality and pass tests.
- [x] Task: Conductor - User Manual Verification 'Phase 1: Scaffolding and DB Command' (Protocol in workflow.md) [checkpoint: e0d0db7]

## Phase 2: Indexing and Search
- [x] Task: Refactor `index` command. 4d5c71f
    - [ ] Write failing tests for `index` command.
    - [ ] Move `index` command logic to `src/leakharvester/cli/commands/index.py`.
    - [ ] Verify functionality and pass tests.
- [x] Task: Refactor `search` command. d0adc17
    - [ ] Write failing tests for `search` command.
    - [ ] Move `search` command logic to `src/leakharvester/cli/commands/search.py`.
    - [ ] Verify functionality and pass tests.
- [x] Task: Conductor - User Manual Verification 'Phase 2: Indexing and Search' (Protocol in workflow.md) [checkpoint: 882d667]

## Phase 3: Ingestion and Maintenance
- [x] Task: Refactor `ingest` command. 39f55ed
    - [ ] Write failing tests for `ingest` command.
    - [ ] Move `ingest` command logic to `src/leakharvester/cli/commands/ingest.py`.
    - [ ] Verify functionality and pass tests.
- [x] Task: Refactor `wipe` and `repair` commands. 0a818dd
    - [ ] Write failing tests for `wipe` and `repair`.
    - [ ] Move logic to `src/leakharvester/cli/commands/wipe.py` and `src/leakharvester/cli/commands/repair.py`.
    - [ ] Verify functionality and pass tests.
- [x] Task: Conductor - User Manual Verification 'Phase 3: Ingestion and Maintenance' (Protocol in workflow.md) [checkpoint: 59c3ff6]

## Phase 4: Info and Secure I/O
- [x] Task: Refactor `info` command. 35f549b
- [x] Task: Refactor `export` and `import` commands. 35f549b
- [x] Task: Conductor - User Manual Verification 'Phase 4: Info and Secure I/O' (Protocol in workflow.md) [checkpoint: e4a5127]

## Phase 5: Final Integration and Cleanup
- [x] Task: Wire up `src/leakharvester/cli/main.py` to include all subcommands.
- [x] Task: Update `pyproject.toml` entry point to point to new CLI location (if applicable, or make `src/leakharvester/main.py` alias the new one). 3af61e4
- [x] Task: Remove legacy code from `src/leakharvester/cli/main.py`. 94aa4f1
- [ ] Task: Run full regression suite and coverage report.
- [ ] Task: Conductor - User Manual Verification 'Phase 5: Final Integration and Cleanup' (Protocol in workflow.md)
