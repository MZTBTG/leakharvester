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
    `_search_blob` String CODEC(ZSTD(9))
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
    """Initializes the ClickHouse database and tables (without heavy indexes)."""
    try:
        settings.create_dirs()
        repo = ClickHouseAdapter()
        
        # Execute DDL statements sequentially
        statements = DDL_SQL.split(";")
        for statement in statements:
            if statement.strip():
                repo.execute_ddl(statement)
        
        log_success("Database initialized successfully (Optimized for Bulk Load).")
    except Exception as e:
        log_error(f"Failed to initialize DB: {e}")
        raise typer.Exit(code=1)

@app.command()
def optimize_db():
    """Adds indexes and materializes them using maximum hardware resources."""
    import time
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
    
    repo = ClickHouseAdapter()
    
    # Aggressive Resource Settings for i5-13600K (20 threads) + 46GB RAM
    # We allocate 30GB to this query to minimize disk flush overhead
    turbo_settings = {
        "max_threads": 20,
        "max_alter_threads": 20,
        "max_memory_usage": 32212254720, # 30 GB
        "mutations_sync": 0 # Async, we will monitor manually
    }
    
    try:
        log_info("Applying Turbo Mode: 20 Threads, 30GB RAM Limit...")
        
        # 1. Add Indexes (Metadata only, fast)
        log_info("Defining Search Index (tokenbf_v1)...")
        repo.client.command(
            "ALTER TABLE vault.breach_records ADD INDEX IF NOT EXISTS idx_search_blob _search_blob TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1",
            settings=turbo_settings
        )
        
        log_info("Defining Email Index (bloom_filter)...")
        repo.client.command(
            "ALTER TABLE vault.breach_records ADD INDEX IF NOT EXISTS idx_email email TYPE bloom_filter(0.01) GRANULARITY 1",
            settings=turbo_settings
        )
        
        # 2. Trigger Materialization (Heavy Work)
        log_info("Triggering Materialization (CPU/IO Bound)...")
        repo.client.command("ALTER TABLE vault.breach_records MATERIALIZE INDEX idx_search_blob", settings=turbo_settings)
        repo.client.command("ALTER TABLE vault.breach_records MATERIALIZE INDEX idx_email", settings=turbo_settings)
        
        # 3. Monitor Progress
        log_info("Mutation started. Monitoring progress...")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            transient=True
        ) as progress:
            task = progress.add_task("[cyan]Building Indexes...", total=100)
            
            while True:
                # Check system.mutations
                # We sum parts_to_do to see how much work is left
                result = repo.client.query(
                    "SELECT sum(parts_to_do) FROM system.mutations WHERE table = 'breach_records' AND is_done = 0"
                ).result_rows
                
                parts_to_do = 0
                
                if result and result[0][0] is not None:
                    parts_to_do = int(result[0][0])
                    
                    if parts_to_do > 0:
                        # We don't have total parts initially, so we can't show accurate % without tracking start.
                        # We'll show a spinner with remaining parts count.
                        progress.update(task, completed=50, description=f"[cyan]Building Indexes: {parts_to_do} parts remaining...")
                    else:
                        progress.update(task, completed=100)
                        break
                else:
                    # No active mutations means we are done
                    progress.update(task, completed=100)
                    break
                
                time.sleep(1)

        log_success("Optimization Complete! Indexes built and optimized.")
        
    except Exception as e:
        log_error(f"Optimization failed: {e}")

@app.command()
def switch_mode(mode: str = typer.Argument(..., help="Index mode: 'eco' (Space-Saver) or 'turbo' (Speed-Demon)")):
    """Switches between index profiles (Eco vs Turbo). Rebuilds indexes."""
    import time
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
    
    if mode not in ["eco", "turbo"]:
        log_error("Invalid mode. Choose 'eco' or 'turbo'.")
        raise typer.Exit(code=1)
    
    repo = ClickHouseAdapter()
    turbo_settings = {
        "max_threads": 20,
        "max_alter_threads": 20,
        "max_memory_usage": 32212254720, # 30 GB
        "mutations_sync": 0
    }
    
    # Configuration
    if mode == "eco":
        # 32KB Bloom Filter (~0.1% storage overhead, slower scans)
        bf_size = 32768
        desc = "Eco Mode (32KB Bloom Filter)"
    else:
        # 256KB Bloom Filter (~1% storage overhead, much lower false positives)
        bf_size = 262144
        desc = "Turbo Mode (256KB Bloom Filter)"
        
    log_info(f"Switching to {desc}...")
    
    try:
        # 1. Drop Existing Indexes
        log_info("Dropping existing indexes...")
        repo.client.command("ALTER TABLE vault.breach_records DROP INDEX IF EXISTS idx_search_blob", settings=turbo_settings)
        repo.client.command("ALTER TABLE vault.breach_records DROP INDEX IF EXISTS idx_email", settings=turbo_settings)
        
        # 2. Add New Indexes
        log_info("Defining new indexes...")
        repo.client.command(
            f"ALTER TABLE vault.breach_records ADD INDEX idx_search_blob _search_blob TYPE tokenbf_v1({bf_size}, 3, 0) GRANULARITY 1",
            settings=turbo_settings
        )
        # Email index usually stays same, but let's rebuild to be clean
        repo.client.command(
            "ALTER TABLE vault.breach_records ADD INDEX idx_email email TYPE bloom_filter(0.01) GRANULARITY 1",
            settings=turbo_settings
        )
        
        # 3. Materialize
        log_info("Materializing new indexes (This will take time)...")
        repo.client.command("ALTER TABLE vault.breach_records MATERIALIZE INDEX idx_search_blob", settings=turbo_settings)
        repo.client.command("ALTER TABLE vault.breach_records MATERIALIZE INDEX idx_email", settings=turbo_settings)
        
        # 4. Monitor
        log_info("Mutation started. Monitoring progress...")
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            transient=True
        ) as progress:
            task = progress.add_task(f"[cyan]Building {mode.title()} Indexes...", total=100)
            
            while True:
                result = repo.client.query(
                    "SELECT sum(parts_to_do) FROM system.mutations WHERE table = 'breach_records' AND is_done = 0"
                ).result_rows
                
                parts_to_do = 0
                if result and result[0][0] is not None:
                    parts_to_do = int(result[0][0])
                    if parts_to_do > 0:
                        progress.update(task, completed=50, description=f"[cyan]Building {mode.title()} Indexes: {parts_to_do} parts remaining...")
                    else:
                        progress.update(task, completed=100)
                        break
                else:
                    progress.update(task, completed=100)
                    break
                time.sleep(1)
                
        log_success(f"Successfully switched to {desc}!")
        
    except Exception as e:
        log_error(f"Mode switch failed: {e}")

@app.command()
def search(
    query: str = typer.Argument(..., help="Search term (e.g. 'augusto.bachini', 'password123')"),
    limit: int = typer.Option(20, help="Max results to display")
):
    """Searches the breach database using optimized token matching."""
    from rich.table import Table
    from rich.console import Console
    import time
    import re
    
    console = Console()
    repo = ClickHouseAdapter()
    
    # Smart Tokenization Strategy
    tokens = [t for t in re.split(r'[^a-zA-Z0-9]', query) if t]
    
    if not tokens:
        log_error("Query contains no valid tokens.")
        return

    # Build WHERE clause
    conditions = [f"hasToken(_search_blob, '{t.lower()}')" for t in tokens]
    where_clause = " AND ".join(conditions)
    
    sql = f"""
        SELECT 
            email, username, password, breach_date, source_file
        FROM vault.breach_records
        WHERE {where_clause}
        LIMIT {limit}
    """
    
    log_info(f"Executing Search: [bold]{query}[/bold]")
    start_time = time.time()
    
    try:
        result = repo.client.query(sql)
        elapsed = time.time() - start_time
        rows = result.result_rows
        cols = result.column_names
        
        if not rows:
            console.print(f"[yellow]No results found for '{query}'.[/yellow] (Time: {elapsed:.2f}s)")
            return
            
        # Display Results
        table = Table(title=f"Search Results ({len(rows)}) - {elapsed:.2f}s")
        for col in cols:
            table.add_column(col, style="cyan")
            
        for row in rows:
            table.add_row(*[str(r) for r in row])
            
        console.print(table)
        
    except Exception as e:
        log_error(f"Search failed: {e}")

@app.command()
def wipe(
    filenames: list[str] = typer.Argument(None, help="List of source filenames to wipe data for"),
    all: bool = typer.Option(False, "--all", help="Wipe ALL data (Truncate Table). Instant space reclamation.")
):
    """Wipes data associated with specific source files or all data."""
    from rich.prompt import Confirm
    
    repo = ClickHouseAdapter()
    
    if all:
        if not Confirm.ask("[bold red]DANGER:[/bold red] This will TRUNCATE the entire database. All data will be lost instantly. Are you sure?"):
            log_info("Wipe operation cancelled.")
            return
        
        log_info("Executing TRUNCATE TABLE (Nuclear Option)...")
        repo.client.command("TRUNCATE TABLE vault.breach_records")
        log_success("Database truncated. Disk space should be reclaimed immediately.")
        return

    if not filenames:
        log_error("No filenames provided. Use --all to wipe everything.")
        return

    # Check if files exist in DB
    files_str = "', '".join(filenames)
    check_sql = f"SELECT count() FROM vault.breach_records WHERE source_file IN ('{files_str}')"
    try:
        count = repo.client.query(check_sql).result_rows[0][0]
        if count == 0:
            log_info(f"No records found for files: {filenames}")
            return
            
        if not Confirm.ask(f"[bold red]WARNING:[/bold red] This will delete {count} records associated with {filenames}. Are you sure?"):
            log_info("Wipe operation cancelled.")
            return
            
        log_info(f"Wiping data for files: {filenames}...")
        
        # Execute DELETE mutation
        delete_sql = f"ALTER TABLE vault.breach_records DELETE WHERE source_file IN ('{files_str}')"
        repo.client.command(delete_sql)
        log_success("Delete mutation submitted.")
        
        log_info("Triggering OPTIMIZE TABLE FINAL to force physical disk cleanup (This may take time)...")
        # Force merge to remove 'inactive' parts left by delete
        repo.client.command("OPTIMIZE TABLE vault.breach_records FINAL")
        log_success("Optimization complete. Disk space reclaimed.")
        
    except Exception as e:
        log_error(f"Wipe operation failed: {e}")

@app.command()
def repair():
    """Attempts to repair and ingest data from the quarantine directory."""
    repo = ClickHouseAdapter()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    ingestor.repair_quarantine(settings.QUARANTINE_DIR, settings.STAGING_DIR)

@app.command()
def ingest(
    file: Path = typer.Option(None, help="Specific file to ingest"),
    stdin: bool = typer.Option(False, help="Ingest from standard input (pipe)"),
    format: str = typer.Option("auto", help="Format hint: 'auto' or 'email:pass' (faster)"),
    no_check: bool = typer.Option(False, help="Disable email validation in Fast Path (Dangerous but Fastest)"),
    batch_size: int = typer.Option(None, help="Batch size (rows) per chunk. Defaults to config (5M)."),
    watch: bool = typer.Option(False, help="Watch raw directory for new files")
):
    """Ingests data from raw directory, specific file, or stdin pipe."""
    import sys
    
    # Use config default if not provided
    final_batch_size = batch_size or settings.BATCH_SIZE
    
    repo = ClickHouseAdapter()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    if stdin:
        # Check if stdin has data
        if sys.stdin.isatty():
            log_error("Stdin is empty. Pipe data into this command: cat file | leakharvester ingest --stdin")
            return
        
        ingestor.process_stream(sys.stdin, settings.STAGING_DIR, settings.QUARANTINE_DIR, batch_size=final_batch_size, format=format, no_check=no_check)
        return

    if file:
        ingestor.process_file(file, settings.STAGING_DIR, settings.QUARANTINE_DIR, batch_size=final_batch_size, format=format, no_check=no_check)
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
                ingestor.process_file(f, settings.STAGING_DIR, settings.QUARANTINE_DIR, batch_size=final_batch_size, format=format, no_check=no_check)

if __name__ == "__main__":
    app()
