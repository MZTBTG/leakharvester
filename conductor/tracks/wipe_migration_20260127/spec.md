# Specification: Migrate Wipe Functionality to DB Command

## 1. Overview
The objective is to consolidate database maintenance operations by migrating all functionality from the standalone `wipe` command into the existing `db` command. This reduces CLI clutter and groups related operations. The `wipe` command will be completely removed.

## 2. Functional Requirements

### 2.1 File Listing (`--lsfiles`)
*   **Command:** `leakharvester db --lsfiles`
*   **Behavior:** Query the ClickHouse database to list all ingested source files.
*   **Output:** Display a formatted **Rich Table** with the following columns:
    *   `Source File`
    *   `Row Count`
    *   `First Import`
    *   `Last Import`
*   **Note:** This replicates the layout style of the `info` command.

### 2.2 Selective Deletion (`--rmfile`)
*   **Command:** `leakharvester db --rmfile <file1>,<file2>...`
*   **Input:** A comma-separated list of filenames.
*   **Validation:**
    1.  Fetch valid `source_file` entries from the database.
    2.  Verify that **ALL** user-provided filenames exist in the valid list.
*   **Error Handling:** If *any* filename is invalid:
    *   Abort the operation.
    *   Print an error message: `Error: File(s) not found: [invalid_names]`.
    *   **Automatically execute** the `--lsfiles` logic to display the full table of valid files (regardless of list size).
*   **Execution:**
    *   If validation passes, prompt: `Are you sure you want to delete [N] file(s)? [y/N]`.
    *   Proceed with deletion only on `y`.

### 2.3 Full Truncate (`--allfiles`)
*   **Command:** `leakharvester db --allfiles`
*   **Behavior:** Remove **ALL** data (Truncate Table) while preserving the table structure.
*   **Safety Mechanism:**
    *   Do not accept simple `y/n` confirmation.
    *   Prompt the user: `Type "wipe" to confirm total data deletion:`
    *   **Validation:** Proceed ONLY if the user types exactly "wipe" (case-sensitive). Any other input or `Ctrl+C` must abort the operation immediately.

### 2.4 cleanup
*   **Action:** Completely remove the `wipe` command source code (`src/leakharvester/cli/commands/wipe.py`) and its registration in the main CLI app.
*   **Result:** Running `leakharvester wipe` should result in the standard Typer/Click "No such command" error.

## 3. Technical Requirements
*   **Security:** Ensure all new SQL queries (especially deletions) use parameterized inputs or safe string handling to prevent injection, despite inputs being filenames.
*   **Reuse:** Leverage existing adapters (`ClickHouseAdapter`) and UI helpers (`Rich` console).

## 4. Out of Scope
*   Data backup before deletion.
*   "Undo" functionality.