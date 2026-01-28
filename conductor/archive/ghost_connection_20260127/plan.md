# Implementation Plan - Ghost Connection Fix & Safe DB Switching

## Phase 1: Settings Integrity & ID Generation [checkpoint: 541d89b]
- [x] Task: Ensure `SettingsManager` handles missing `lh-settings.json` gracefully. [76b2452]
    - [x] Sub-task: Write tests for `SettingsManager` verifying behavior when `lh-settings.json` is missing (should create default). [76b2452]
    - [x] Sub-task: Implement creation logic in `SettingsManager` to generate the file in User Home/Installation paths if absent. [76b2452]
- [x] Task: Add `instance_id` to settings schema. [4322c2d]
    - [x] Sub-task: Update `Settings` model to include an optional `instance_id` (UUID). [4322c2d]
    - [x] Sub-task: Update tests to verify `instance_id` persistence and retrieval. [4322c2d]
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md)

## Phase 2: Robust DB Initialization (Docker Lifecycle) [checkpoint: a318fe6]
- [x] Task: Refactor `db --init` command to enforce container recreation. [13845e2]
    - [x] Sub-task: Write tests mocking `docker compose` calls to verify the correct sequence: `down` -> `up --force-recreate`. [13845e2]
    - [x] Sub-task: Modify `commands/db.py` to execute `docker compose down` before starting. [13845e2]
    - [x] Sub-task: Ensure the new `active_db_path` is correctly written to `.env` or passed to the subprocess before restart. [13845e2]
- [x] Task: Implement Server-Side Instance ID Storage. [d81d4ff]
    - [x] Sub-task: Create a migration or init script to create a `system_info` table in ClickHouse. [d81d4ff]
    - [x] Sub-task: Update `db --init` to generate a new UUID, save it to `lh-settings.json`, and insert it into the `system_info` table. [d81d4ff]
- [ ] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md)

## Phase 3: Safe Connection Adapter [REMOVED] [checkpoint: a5c146c]
- [x] Task: Implement "Safe Fail" logic in `ClickHouseAdapter`. [bb5fcca]
    - [x] Sub-task: Define `EnvironmentMismatchError` in `domain/exceptions.py`. [bb5fcca]
    - [x] Sub-task: Write integration tests. [bb5fcca]
    - [x] Sub-task: Modify `ClickHouseAdapter.connect()` (or `__init__`) to query `system_info`, fetch the server ID, and compare with `Settings.instance_id`. [bb5fcca]
    - [x] **NOTE:** Feature reverted in [2ebf2a9] per user request ("Remove the local_ID implementation"). Relying on Docker recreation (Phase 2) for safety.
- [x] Task: Conductor - User Manual Verification 'Phase 3' (Protocol in workflow.md)
