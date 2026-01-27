# Implementation Plan - Refine CLI Visual Styling (Adaptive Width)

## Phase 1: Setup & Configuration
- [x] Task: Add `rich-click` dependency to `pyproject.toml`
    - [x] Sub-task: Execute `uv add rich-click`.
    - [x] Sub-task: Verify installation via `uv run pip list | grep rich-click`.
- [x] Task: Configure Global Styles in `src/leakharvester/cli/main.py`
    - [x] Sub-task: Import `rich_click` and `rich.console`.
    - [x] Sub-task: Set `rich_click.rich_click.STYLE_HELP_PATH_MAX_WIDTH` to a reasonable default (e.g., 80) or ensure adaptive width logic.
    - [x] Sub-task: **CRITICAL:** Set `rich_click.rich_click.MAX_WIDTH` or related config to prevent full-width expansion.
    - [x] Sub-task: Configure `rich_click.rich_click.STYLE_ERRORS_PANEL_BORDER` and `STYLE_ERRORS_PANEL_TITLE` to align with "Adaptive" requirement.
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Setup & Configuration' (Protocol in workflow.md)
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Setup & Configuration' (Protocol in workflow.md)

## Phase 2: Error & Response Refactoring
- [ ] Task: Refactor Custom `log_error` in `src/leakharvester/adapters/console.py`
    - [ ] Sub-task: Import `Panel` from `rich.panel`.
    - [ ] Sub-task: Update `log_error` to print a `Panel` with `expand=False`.
    - [ ] Sub-task: Ensure the title is "ERROR" and style is "bold red".
- [ ] Task: Verify Typer/Click Error Output
    - [ ] Sub-task: Create a temporary test script or manual verification step to trigger a Typer usage error (e.g., missing argument).
    - [ ] Sub-task: Confirm that `rich-click` is automatically handling these errors with the configured adaptive style.
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Error & Response Refactoring' (Protocol in workflow.md)

## Phase 3: Final Polish & Cleanup
- [ ] Task: Review and Polish `--help` Output
    - [ ] Sub-task: Run `leakharvester --help` and subcommands (`db --help`).
    - [ ] Sub-task: Tweak `rich-click` settings if help panels are still too wide or misaligned.
- [ ] Task: Run Full Test Suite
    - [ ] Sub-task: Ensure no regressions in existing functional tests.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Final Polish & Cleanup' (Protocol in workflow.md)