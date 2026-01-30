# LeakHarvester

**High-Performance Breach Data Ingestion & Search Engine.**

LeakHarvester is a specialized CLI tool designed for security researchers and forensic analysts. It ingests massive, unstructured breach datasets (CSV, Text) into a highly optimized ClickHouse backend, enabling sub-second full-text searches across billions of records.

> **Status:** Production Ready (v1.0.0)

## 🏗 Architecture

LeakHarvester is built on a Hexagonal Architecture (Ports & Adapters) to separate core domain logic from infrastructure.

*   **Core:** Polars-based streaming ingestion pipeline with heuristic schema normalization.
*   **Storage:** ClickHouse (via `clickhouse-connect`) using `MergeTree` engines with ZSTD(3) compression and Delta encoding.
*   **IO Layer:** Apache Arrow IPC streams for high-throughput data transfer.
*   **Security:** `Argon2id` + `XChaCha20-Poly1305` encryption for portable `.lh` data containers.

[View Full Architecture Documentation](docs/src/leakharvester/cli/main.md)

## 🚀 Installation

### Prerequisites
*   **Python 3.12+**
*   **Docker** (for the ClickHouse backend)
*   **uv** (recommended) or `pip`

### Setup

1.  **Clone and Sync:**
    ```bash
    git clone https://github.com/your-org/leakharvester.git
    cd leakharvester
    uv sync
    ```

2.  **Start the Database:**
    LeakHarvester manages its own Docker container.
    ```bash
    uv run leakharvester db --init
    ```
    *This will spin up ClickHouse, create the `vault` database, and apply the schema.*

## ⚡ Quick Start

### 1. Ingest Data
Drop your raw `.txt` or `.csv` breach files into `data/raw`, or ingest a specific file directly. The engine automatically detects delimiters, headers, and maps columns like `email`, `password`, `username`.

```bash
# Ingest all files in data/raw
uv run leakharvester ingest

# Ingest a specific file (Auto-detect format)
uv run leakharvester ingest --file ./breach_dump.txt

# Ingest from stdin (Pipe)
cat huge_breach.txt | uv run leakharvester ingest --stdin --format "email:pass"
```

### 2. Search
Perform instant searches. By default, it uses a fuzzy `ILIKE` search.

```bash
# Fuzzy search (case-insensitive)
uv run leakharvester search "company.com"

# Exact match (faster)
uv run leakharvester search -e "ceo@company.com"

# Search specific columns
uv run leakharvester search "password123" --column password
```

### 3. Smart Indexing (Optimization)
Don't rely on brute-force scans. Use the heuristic analyzer to recommend and apply the best indices (Inverted, Bloom Filters, N-Grams) based on column statistics.

```bash
# Run analysis and interactive optimization
uv run leakharvester index --auto-optimize
```

### 4. Secure Transport (.lh)
Need to move sensitive data? Export it to a `.lh` container—a cryptographically secure, compressed archive.

```bash
# Export to encrypted file (Prompts for password)
uv run leakharvester export -o ./evidence.lh --compression-level 10

# Import back into another instance
uv run leakharvester import -i ./evidence.lh
```

## 📚 Command Reference

### `db` - Database Lifecycle
Manage the ClickHouse instance and local configuration.
*   `--init`: Start Docker and apply DDL schema.
*   `--status`: Check connection and row counts.
*   `--lsfiles`: List ingested source files and statistics.
*   `--rmfile`: Wipe data associated with specific files.
*   `--reset-all`: **Factory Reset** (Wipes config, data, and containers).

### `ingest` - Data Pipeline
*   **Format Detection:** Uses `csv.Sniffer` and Regex heuristics to identify `email`, `password`, `username`.
*   **Staging:** Loads data into temporary tables first, then atomically swaps partitions to ensure consistency.
*   **Quarantine:** Malformed rows are isolated in `data/quarantine` (Parquet format) for manual inspection or repair.

### `repair` - Quarantine Recovery
Scans the quarantine directory and attempts aggressive regex recovery to salvage valid records from corrupted lines.
```bash
uv run leakharvester repair
```

### `index` - Performance Tuning
*   `--auto-optimize`: The recommended way to manage indexes.
*   **Manual Types:**
    *   `--tokenbf`: Token Bloom Filter (Good for emails, usernames).
    *   `--ngram`: N-Gram Bloom Filter (Good for substring search in passwords).
    *   `--inverted`: Full Inverted Index (Best for general text search).

### `info` - Dashboard
Displays a terminal dashboard with database health, storage efficiency (compression ratios), active indices, and top data sources.

## 📂 Documentation Index

Detailed technical documentation is available for every module:

### Core & Domain
*   [Configuration](docs/src/leakharvester/config.md)
*   [Domain Schemas](docs/src/leakharvester/domain/schemas.md)
*   [Business Rules](docs/src/leakharvester/domain/rules.md)

### Services
*   [Ingestor Service](docs/src/leakharvester/services/ingestor.md) - The ETL engine.
*   [Index Optimizer](docs/src/leakharvester/services/index_optimizer.md) - Heuristic indexer.
*   [Secure IO](docs/src/leakharvester/services/secure_io.md) - Crypto implementation (`.lh` format).

### Adapters (Infrastructure)
*   [ClickHouse Adapter](docs/src/leakharvester/adapters/clickhouse.md)
*   [Local Filesystem](docs/src/leakharvester/adapters/local_fs.md)

### CLI Commands
*   [Main Entry](docs/src/leakharvester/cli/main.md)
*   [DB Command](docs/src/leakharvester/cli/commands/db.md)
*   [Ingest Command](docs/src/leakharvester/cli/commands/ingest.md)
*   [Search Command](docs/src/leakharvester/cli/commands/search.md)
*   [Index Command](docs/src/leakharvester/cli/commands/index.md)
*   [Secure IO Command](docs/src/leakharvester/cli/commands/secure_io.md)