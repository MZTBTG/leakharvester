# LeakHarvester

**High-Performance Breach Data Ingestion & Search Engine.**

LeakHarvester is a specialized CLI tool designed for manage huge datasets and perform searches over them, without compromissing your storage space or collecting any kind of data. It ingests massive, unstructured breach datasets (CSV, Text and more on the future) into a highly optimized ClickHouse backend, enabling sub-second full-text searches across billions of records.

## Architecture

LeakHarvester is built on a Hexagonal Architecture (Ports & Adapters) to separate core domain logic from infrastructure.

*   **Core:** Polars-based streaming ingestion pipeline with heuristic schema normalization.
*   **Storage:** ClickHouse (via `clickhouse-connect`) using `MergeTree` engines with ZSTD(3) compression and Delta encoding.
*   **IO Layer:** Apache Arrow IPC streams for high-throughput data transfer.
*   **Security:** `Argon2id` + `XChaCha20-Poly1305` encryption for portable `.lh` data containers, easily shareable with anyone with security.

[View Full Architecture Documentation (Soon...)](about:blank)

## Installation

### Prerequisites
*   **Python 3.12+**
*   **Docker** (for the ClickHouse backend)
*   **uv** (recommended) or `pip`

### Setup

1. **Installing and configuring the dependencies:**
    - **Basic dependencies:**
    ```bash
    sudo apt update && \
    sudo apt install python3-venv git curl apt-transport-https ca-certificates gnupg
    ```
    
    - **Docker config (Example for Debian systems. For other dists, check https://docs.docker.com/engine/install/):**
    ```bash
    sudo install -m 0755 -d /etc/apt/keyrings && \
    sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc && \
    sudo chmod a+r /etc/apt/keyrings/docker.asc && \
    sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
    Types: deb
    URIs: https://download.docker.com/linux/debian
    Suites: $(. /etc/os-release && echo "$VERSION_CODENAME")
    Components: stable
    Signed-By: /etc/apt/keyrings/docker.asc
    EOF

    sudo apt update && \
    sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin # If some error occur, run `sudo apt --fix-broken install`
    sudo systemctl enable --now docker && \
    sudo systemctl enable containerd.service && \
    sudo groupadd docker && \
    sudo usermod -aG docker $USER && \
    newgrp docker
    ```

    - **UV installation:**
    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    echo 'export PATH=$PATH:$HOME/.local/bin' >> ~/.profile && \
    source $HOME/.profile
    ```
    
    - **ClickHouse config and install:**
    ```bash
    curl -fsSL 'https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key' | \
    sudo gpg --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg && \
    ARCH=$(dpkg --print-architecture) && \
    echo "deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=${ARCH}] https://packages.clickhouse.com/deb stable main" | \
    sudo tee /etc/apt/sources.list.d/clickhouse.list && \
    sudo apt update && \
    sudo apt install clickhouse-server clickhouse-client
    ```

2.  **Clone and Sync:**
    ```bash
    git clone https://github.com/mztbtg/leakharvester.git && \
    cd leakharvester && \
    uv sync && \
    uv tool install .
    ```

3.  **Start the Database:**
    LeakHarvester manages its own Docker container.
    ```bash
    leakharvester db --init
    ```
    *This will spin up ClickHouse, create the `vault` database, and apply the schema.*

## ⚡ Quick Start

### 1. Ingest Data
Ingest a specific text or `csv` file directly, or drop your raw breach files into `data/raw`. The engine automatically detects delimiters, headers, and maps columns like `email`, `password`, `username`, but you are able to configure it for yourself.

```bash
# Show help
leakharvester ingest --help

# Ingest all files in data/raw
leakharvester ingest

# Ingest a specific file (Auto-detect format)
leakharvester ingest --file ./breach_dump.txt

# Ingest from stdin (Pipe)
cat huge_breach.txt | leakharvester ingest --stdin --format "doc:email:pass"
```

### 2. Search
Perform instant searches. By default, it uses a fuzzy `ILIKE` search on all relevant columns.
Use `lhs` command as an alias of `leakharvester search`.

```bash
# Show help
leakharvester search --help
lhs --help

# Fuzzy search (case-insensitive)
leakharvester search "company.com"
lhs company.com

# Exact match (faster)
lhs -e "ceo@company.com"

# Search specific columns
lhs "password123" --column password

# Instant search (Exact case-sensitive match)
lhs "ceo@somecompany.com" -C -e -c email
```

### 3. Smart Indexing (Optimization)
Don't rely on brute-force scans. Use the heuristic analyzer to recommend and apply the best indices (Inverted, Bloom Filters, N-Grams) based on column statistics.

```bash
# Run analysis and interactive optimization
leakharvester index --auto-optimize
```

### 4. Secure Transport (.lh)
Need to move sensitive data? Export it to a `.lh` container—a cryptographically secure, compressed archive.

```bash
# Export to encrypted file (Prompts for password)
leakharvester export -o ./evidence.lh --compression-level 10

# Import back into another instance
leakharvester import -i ./evidence.lh
```

## Command Reference

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
leakharvester repair
```

### `index` - Performance Tuning
*   `--auto-optimize`: The recommended way to manage indexes.
*   **Manual Types:**
    *   `--tokenbf`: Token Bloom Filter (Good for emails, usernames).
    *   `--ngram`: N-Gram Bloom Filter (Good for substring search in passwords).
    *   `--inverted`: Full Inverted Index (Best for general text search).

### `info` - Dashboard
Displays a terminal dashboard with database health, storage efficiency (compression ratios), active indices, and top data sources.

## Documentation Index

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