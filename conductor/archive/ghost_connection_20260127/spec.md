# Specification: Ghost Connection Fix & Safe DB Switching

## Overview
This track addresses a critical bug where the `db --init` command fails to correctly switch the active database environment in the Docker backend. This results in "Ghost Connections" where the CLI is configured for a new path, but the backend service (ClickHouse) continues serving the old data volume. We will implement a robust fix ensuring container recreation and add a "Safe Fail" mechanism using unique Instance IDs.

## Problem Statement
When a user initializes a new database at a custom path using `db --path ... --init`, the existing Docker container is not torn down or forced to recreate. Consequently, the volume mount remains pointing to the previous location. The CLI connects to `localhost:8123`, unaware it is communicating with the old database instance, leading to data ingestion into the wrong repository.

## Functional Requirements

### 1. Robust `db --init` Orchestration
*   **Stop Before Start:** The `init` command MUST execute `docker compose down` (or equivalent) to stop and remove the running container before starting a new one.
*   **Force Recreate:** The command to start the container MUST use flags (e.g., `--force-recreate`) to ensure the new volume configuration from `.env` or the environment context is picked up.
*   **Environment Consistency:** Ensure the `.env` file or environment variables passed to Docker Compose correctly reflect the *new* `active_db_path`.

### 2. Instance Identity Verification (Safety Mechanism)
*   **ID Generation:** Upon successful initialization of a new database (via `db --init`), a unique **Instance ID** (UUID) must be generated.
*   **Dual Storage:**
    1.  **Client-Side:** Store this ID in the `lh-settings.json`.
    2.  **Server-Side:** Store this ID persistently within the ClickHouse database (e.g., in a `system_info` table or a specific `config` table).
*   **Settings File Integrity:**
    *   The system MUST check for the existence of `lh-settings.json`.
    *   If missing, it MUST be created in the appropriate locations (User Home directory and/or Installation Local directory) with default values before proceeding.
*   **Connection Validation:** The `ClickHouseAdapter` MUST retrieve the server-side Instance ID upon connection and compare it with the client-side configured ID.
*   **Safe Fail:** If the IDs do not match, the adapter MUST abort the connection and raise a clear `EnvironmentMismatchError`, preventing any read/write operations.

## Non-Functional Requirements
*   **UX:** The error message for a mismatch should guide the user to restart the service or check their configuration.
*   **Performance:** The ID check should be a lightweight query executed only once per connection session (or connection pool initialization).

## Acceptance Criteria
1.  **Context Switch Test:**
    *   Initialize DB A at Path A. Ingest Data.
    *   Initialize DB B at Path B. Ingest Data.
    *   Verify that data meant for B is **only** in B, and A is untouched.
2.  **Volume verification:**
    *   Running `docker inspect` after a switch shows the container mounted to the new host path.
3.  **Safety Check Test:**
    *   Manually point the CLI config to Path A while the Docker container for Path B is running.
    *   Attempting an operation MUST fail with an `EnvironmentMismatchError`.
4.  **Settings File Recovery:**
    *   Delete `lh-settings.json`. Run a command. Verify the file is recreated and the operation succeeds.
