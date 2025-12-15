import typer
from pathlib import Path
from leakharvester.config import settings
from leakharvester.adapters.clickhouse import ClickHouseAdapter
from leakharvester.adapters.local_fs import LocalFileSystemAdapter
from leakharvester.services.ingestor import BreachIngestor
from leakharvester.adapters.console import log_info, log_success, log_error

app = typer.Typer(name="leakharvester", help="High-Performance Breach Data Ingestion Engine")

DDL_SQL = """
CREATE DATABASE IF NOT EXISTS vault;

CREATE TABLE IF NOT EXISTS vault.breach_records
(
    `source_file` LowCardinality(String) CODEC(ZSTD(9)),
    `breach_date` Date CODEC(Delta(2), ZSTD(9)),
    `import_date` DateTime DEFAULT now() CODEC(Delta(4), ZSTD(9)),
    `email` String CODEC(ZSTD(9)),
    `username` String CODEC(ZSTD(9)),
    `password` String CODEC(ZSTD(9)),
    `hash` String CODEC(ZSTD(9)),
    `salt` String CODEC(ZSTD(9)),
    `_search_blob` String CODEC(ZSTD(9)),
    INDEX idx_search_blob _search_blob TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    INDEX idx_email email TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (email, source_file)
PARTITION BY toYYYYMM(breach_date)
SETTINGS
    index_granularity = 8192,
    max_bytes_to_merge_at_min_space_in_pool = 10485760,
    min_bytes_for_wide_part = 10485760;
"""

@app.command()
def init_db():
    """Initializes the ClickHouse database and tables."""
    try:
        settings.create_dirs()
        repo = ClickHouseAdapter()
        
        # Execute DDL statements sequentially
        statements = DDL_SQL.split(";")
        for statement in statements:
            if statement.strip():
                repo.execute_ddl(statement)
        
        log_success("Database initialized successfully.")
    except Exception as e:
        log_error(f"Failed to initialize DB: {e}")
        raise typer.Exit(code=1)

@app.command()
def ingest(
    file: Path = typer.Option(None, help="Specific file to ingest"),
    watch: bool = typer.Option(False, help="Watch raw directory for new files")
):
    """Ingests data from raw directory or specific file."""
    repo = ClickHouseAdapter()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    if file:
        ingestor.process_file(file, settings.STAGING_DIR, settings.QUARANTINE_DIR)
    else:
        # Process all files in raw
        # In real-world, we'd use robust walking or a queue.
        # For this prototype: list dir
        files = list(settings.RAW_DIR.glob("*"))
        if not files:
            log_info("No files found in raw directory.")
            return

        for f in files:
            if f.is_file():
                ingestor.process_file(f, settings.STAGING_DIR, settings.QUARANTINE_DIR)

if __name__ == "__main__":
    app()
