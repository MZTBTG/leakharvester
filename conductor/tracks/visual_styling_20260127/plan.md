# Implementation Plan - Refine CLI Visual Styling (Adaptive Width)

## Phase 1: Setup & Configuration [checkpoint: 8837015]
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

## Phase 2: Error & Response Refactoring [checkpoint: 5ecb367]
- [x] Task: Refactor Custom `log_error` in `src/leakharvester/adapters/console.py`
    - [x] Sub-task: Import `Panel` from `rich.panel`.
    - [x] Sub-task: Update `log_error` to print a `Panel` with `expand=False`.
    - [x] Sub-task: Ensure the title is "ERROR" and style is "bold red".
- [x] Task: Verify Typer/Click Error Output
    - [x] Sub-task: Create a temporary test script or manual verification step to trigger a Typer usage error (e.g., missing argument).
    - [x] Sub-task: Confirm that `rich-click` is automatically handling these errors with the configured adaptive style.
- [x] Task: Conductor - User Manual Verification 'Phase 2: Error & Response Refactoring' (Protocol in workflow.md)

## Phase 3: Final Polish & Cleanup
- [x] Task: Review and Polish `--help` Output
    - [x] Sub-task: Run `leakharvester --help` and subcommands (`db --help`).
    - [x] Sub-task: Tweak `rich-click` settings if help panels are still too wide or misaligned.
- [x] Task: Run Full Test Suite
    - [x] Sub-task: Ensure no regressions in existing functional tests.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: Final Polish & Cleanup' (Protocol in workflow.md)