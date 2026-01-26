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
- [ ] Task: Refactor `index` command.
    - [ ] Write failing tests for `index` command.
    - [ ] Move `index` command logic to `src/leakharvester/cli/commands/index.py`.
    - [ ] Verify functionality and pass tests.
- [ ] Task: Refactor `search` command.
    - [ ] Write failing tests for `search` command.
    - [ ] Move `search` command logic to `src/leakharvester/cli/commands/search.py`.
    - [ ] Verify functionality and pass tests.
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Indexing and Search' (Protocol in workflow.md)

## Phase 3: Ingestion and Maintenance
- [ ] Task: Refactor `ingest` command.
    - [ ] Write failing tests for `ingest` command.
    - [ ] Move `ingest` command logic to `src/leakharvester/cli/commands/ingest.py`.
    - [ ] Verify functionality and pass tests.
- [ ] Task: Refactor `wipe` and `repair` commands.
    - [ ] Write failing tests for `wipe` and `repair`.
    - [ ] Move logic to `src/leakharvester/cli/commands/wipe.py` and `src/leakharvester/cli/commands/repair.py`.
    - [ ] Verify functionality and pass tests.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Ingestion and Maintenance' (Protocol in workflow.md)

## Phase 4: Info and Secure I/O
- [ ] Task: Refactor `info` command.
    - [ ] Write failing tests for `info` command.
    - [ ] Move `info` logic to `src/leakharvester/cli/commands/info.py`.
    - [ ] Verify functionality and pass tests.
- [ ] Task: Refactor `export` and `import` commands.
    - [ ] Write failing tests for `export` and `import`.
    - [ ] Move logic to `src/leakharvester/cli/commands/secure_io.py`.
    - [ ] Verify functionality and pass tests.
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Info and Secure I/O' (Protocol in workflow.md)

## Phase 5: Final Integration and Cleanup
- [ ] Task: Wire up `src/leakharvester/cli/main.py` to include all subcommands.
- [ ] Task: Update `pyproject.toml` entry point to point to new CLI location (if applicable, or make `src/leakharvester/main.py` alias the new one).
- [ ] Task: Remove legacy code from `src/leakharvester/main.py`.
- [ ] Task: Run full regression suite and coverage report.
- [ ] Task: Conductor - User Manual Verification 'Phase 5: Final Integration and Cleanup' (Protocol in workflow.md)
