# LeakHarvester Update Plan

## Phase 1: Ingestor Refactoring (Dynamic Format & Schema)
- [x] **Remove `_search_blob`**: Eliminate all logic related to creating and storing `_search_blob` in `src/leakharvester/services/ingestor.py`.
- [x] **Implement `_parse_format_string`**: Add helper method in `ingestor.py` to parse formats like `email:pass:document` and detect delimiters.
- [x] **Implement `_validate_and_sync_schema`**: Add logic to check ClickHouse columns against the parsed format and prompt user to add missing columns.
- [x] **Update `_process_raw_streaming`**: Rewrite to use the new dynamic format parser, split logic, and schema validation.
- [x] **Update `process_stream`**: Align stdin processing with the same dynamic logic.

## Phase 2: Main CLI Updates
- [x] **Verify `search` command**: Ensure it searches across columns (ILIKE) and not `_search_blob`.
- [x] **Update `ingest` command**: Ensure arguments are passed correctly to the updated ingestor methods.

## Phase 3: Improvements
- [x] **Update Progress Logging**: Show cumulative row count and elapsed time.
- [ ] **Manual Test**: Run dry-run ingest with custom format to verify schema prompting and data insertion.
