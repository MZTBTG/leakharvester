# Implementation Plan - Migrate Wipe Functionality to DB Command

## Phase 1: Setup & TDD
- [x] Task: Create new test file `tests/test_cli_db_migration.py` [b24a777]
    - [x] Sub-task: Scaffold test file with `ClickHouseAdapter` mocks.
- [x] Task: Write failing tests for `--lsfiles` [b24a777]
    - [x] Sub-task: Test table output formatting (Rich Console).
    - [x] Sub-task: Test empty database state.
- [x] Task: Write failing tests for `--rmfile` [b24a777]
    - [x] Sub-task: Test successful deletion of single and multiple files.
    - [x] Sub-task: Test validation error (file not found) and automatic table listing.
    - [x] Sub-task: Test user cancellation at confirmation prompt.
- [x] Task: Write failing tests for `--allfiles` [b24a777]
    - [x] Sub-task: Test successful truncation with correct confirmation input ("wipe").
    - [x] Sub-task: Test abortion on incorrect confirmation input.
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Setup & TDD' (Protocol in workflow.md)

## Phase 2: Implementation
- [ ] Task: Implement `--lsfiles` in `src/leakharvester/cli/commands/db.py`
    - [ ] Sub-task: Add `lsfiles` boolean flag to `db_command`.
    - [ ] Sub-task: Implement SQL query to fetch file stats.
    - [ ] Sub-task: Render Rich Table with columns: Source File, Row Count, First Import, Last Import.
- [ ] Task: Implement `--rmfile` in `src/leakharvester/cli/commands/db.py`
    - [ ] Sub-task: Add `rmfile` string argument (comma-separated).
    - [ ] Sub-task: Implement validation logic (fetch valid files, check subset).
    - [ ] Sub-task: Implement error handling: Print error -> Run `lsfiles` logic.
    - [ ] Sub-task: Implement deletion logic using `ALTER TABLE ... DELETE`.
- [ ] Task: Implement `--allfiles` in `src/leakharvester/cli/commands/db.py`
    - [ ] Sub-task: Add `allfiles` boolean flag.
    - [ ] Sub-task: Implement "Type 'wipe' to confirm" safety prompt.
    - [ ] Sub-task: Implement `TRUNCATE TABLE` execution.
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Implementation' (Protocol in workflow.md)

## Phase 3: Deprecation & Cleanup
- [ ] Task: Remove `wipe` command
    - [ ] Sub-task: Delete `src/leakharvester/cli/commands/wipe.py`.
    - [ ] Sub-task: Remove `wipe` command registration from `src/leakharvester/cli/main.py`.
- [ ] Task: Verify Clean State
    - [ ] Sub-task: Run full test suite to ensure no regressions.
    - [ ] Sub-task: Verify `leakharvester --help` does not show `wipe`.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Deprecation & Cleanup' (Protocol in workflow.md)