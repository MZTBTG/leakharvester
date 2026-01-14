# LeakHarvester

High-Performance Breach Data Ingestion & Search Engine powered by ClickHouse.

## Features

*   **Ultra-Fast Ingestion:** Bulk loads CSV/JSON data into ClickHouse with automatic deduplication.
*   **Secure Containers (.lh):** Import/Export data using military-grade encryption (Argon2id + XChaCha20) and ZSTD compression.
*   **Smart Indexing (Eco Mode):** Automatically manages storage-efficient Inverted Indexes on all String columns for sub-second full-text search.
*   **Turbo Mode (Legacy):** Optional Bloom Filter indexing for specific use cases (deprecated/manual).
*   **Search CLI:** Instant substring and full-text search capabilities with regex support.
*   **Auto-Repair:** Detects and fixes index inconsistencies during mode switches.

## Quick Start

### 1. Initialize
Initialize the database (requires ClickHouse running).
```bash
uv run python -m leakharvester.main init-db
```

### 2. Ingest Data
Ingest raw breach files from the `data/raw` directory.
```bash
uv run python -m leakharvester.main ingest
```

### 3. Secure Data Transport (.lh)
Export sensitive data to an encrypted, compressed container.

**Export (Encrypt & Compress):**
```bash
# Exports all data to a secure file. You will be prompted for a password.
uv run python -m leakharvester.main export -o ./dump.lh
```

**Export (Plaintext & Filtered):**
```bash
# Exports specific columns without encryption
uv run python -m leakharvester.main export -o ./emails.lh --no-pass -c "email,breach_date"
```

**Import:**
```bash
# Imports data back into the vault. Detects encryption automatically.
uv run python -m leakharvester.main import -i ./dump.lh
```

### 4. Optimize (Eco Mode)
Enable high-performance Inverted Indexes on all text columns (`email`, `username`, `password`, etc.).
This command automatically:
*   Builds missing indexes.
*   Removes legacy/garbage indexes.
*   Defragments data for optimal storage.
```bash
uv run python -m leakharvester.main switch-mode eco
```

### 5. Search
Search across all indexed columns.
```bash
uv run python -m leakharvester.main search "augusto.bachini"
```

## Architecture

*   **Database:** ClickHouse (MergeTree Engine)
*   **Secure Container (.lh):** 
    *   **Format:** Custom binary container with Magic Bytes `LH01`.
    *   **Cryptography:** Argon2id Key Derivation + XChaCha20-Poly1305 (PyNaCl).
    *   **Transport:** Apache Arrow IPC Stream compressed with Zstandard (Parallel).
*   **Indexes:** 
    *   **Primary Key:** `(email, source_file)` for instant exact lookups.
    *   **Eco Mode:** `inverted(0)` (Inverted Index) on all String columns. Greatly accelerates `hasToken` and token-based `ILIKE`.
*   **Storage:** ZSTD(3) Compression with Delta encoding for dates.

## Troubleshooting

*   **Low Storage?** Run `switch-mode eco` to trigger a defragmentation and garbage collection pass.
*   **Slow Search?** Ensure you are in Eco Mode. Unindexed columns will trigger a warning.
