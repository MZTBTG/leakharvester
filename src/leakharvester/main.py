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
    `source_file` LowCardinality(String) CODEC(ZSTD(3)),
    `breach_date` Date CODEC(Delta(2), ZSTD(3)),
    `import_date` DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3)),
    `email` String CODEC(ZSTD(3)),
    `username` String CODEC(ZSTD(3)),
    `password` String CODEC(ZSTD(3))
)
ENGINE = MergeTree
ORDER BY (email, source_file)
PARTITION BY source_file
SETTINGS
    index_granularity = 8192,
    max_bytes_to_merge_at_min_space_in_pool = 10485760,
    min_bytes_for_wide_part = 10485760,
    old_parts_lifetime = 60,
    max_partitions_per_insert_block = 1000;
"""

@app.command()
def init_db(
    reset: bool = typer.Option(False, "--reset", help="DROP existing table and recreate schema (Data Loss!)")
):
    """Initializes the ClickHouse database and tables (without heavy indexes)."""
    try:
        settings.create_dirs()
        repo = ClickHouseAdapter()
        
        if reset:
            from rich.prompt import Confirm
            if Confirm.ask("[bold red]WARNING:[/bold red] This will DROP the existing database/table. All data will be lost. Continue?"):
                repo.execute_ddl("DROP TABLE IF EXISTS vault.breach_records")
                log_info("Dropped existing table.")
            else:
                log_info("Reset cancelled.")
                return
        
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
        
        # 1. Add Indexes
        log_info("Defining Email Index (ngrambf_v1 - Trigram)...")
        repo.client.command(
            "ALTER TABLE vault.breach_records ADD INDEX IF NOT EXISTS idx_email email TYPE ngrambf_v1(3, 32768, 2, 0) GRANULARITY 1",
            settings=turbo_settings
        )
        
        # 2. Trigger Materialization (Heavy Work)
        log_info("Triggering Materialization (CPU/IO Bound)...")
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
        # 32KB Bloom Filter (~0.1% storage overhead)
        bf_size = 32768
        desc = "Eco Mode (32KB Trigram Index)"
    else:
        # 256KB Bloom Filter (~1% storage overhead, fewer false positives)
        bf_size = 262144
        desc = "Turbo Mode (256KB Trigram Index)"
        
    log_info(f"Switching to {desc}...")
    
    try:
        # 1. Drop Existing Indexes
        log_info("Dropping existing indexes...")
        repo.client.command("ALTER TABLE vault.breach_records DROP INDEX IF EXISTS idx_email", settings=turbo_settings)
        
        # 2. Add New Indexes
        log_info("Defining new indexes...")
        
        # Use ngrambf_v1 for ILIKE support
        repo.client.command(
            f"ALTER TABLE vault.breach_records ADD INDEX idx_email email TYPE ngrambf_v1(3, {bf_size}, 2, 0) GRANULARITY 1",
            settings=turbo_settings
        )
        
        # 3. Materialize
        log_info("Materializing new indexes (This will take time)...")
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
    query: str = typer.Argument(None, help="Search term (e.g. 'augusto.bachini', 'password123')"),
    limit: int = typer.Option(20, help="Max results to display"),
    column: str = typer.Option(None, help="Comma-separated list of columns to search (default: all)"),
    columns: bool = typer.Option(False, help="List available columns")
):
    """Searches the breach database using ILIKE across all string columns."""
    from rich.table import Table
    from rich.console import Console
    from rich.panel import Panel
    from rich import box
    import time
    
    console = Console()
    repo = ClickHouseAdapter()
    
    # Fetch existing columns dynamically
    try:
        all_cols = repo.get_columns("vault.breach_records")
    except Exception as e:
        log_error(f"Failed to fetch table schema: {e}")
        return

    # Handle introspection request
    if columns:
        schema_text = ", ".join([f"[green]{c}[/green]" for c in all_cols])
        schema_panel = Panel(
            schema_text, 
            title="[bold green]Available Columns[/bold green]", 
            border_style="green",
            box=box.ROUNDED
        )
        console.print(schema_panel)
        return

    # If we are not listing columns, query is required
    if not query:
        console.print("[red]Error: Missing argument 'QUERY'.[/red]")
        raise typer.Exit(code=1)

    # Determine columns to search
    if column:
        search_cols = [c.strip() for c in column.split(",")]
        # Validate existence
        invalid = [c for c in search_cols if c not in all_cols]
        if invalid:
            log_error(f"Columns not found: {invalid}")
            return
    else:
        # Default: Search all String-like columns (heuristic: skip dates/metadata if needed)
        # We search everything except dates usually, but let's just search everything that is likely text.
        # Ideally we check types, but for now we assume most user cols are strings.
        # We skip internal ClickHouse metadata if any.
        search_cols = [c for c in all_cols if c not in ('breach_date', 'import_date')]
        log_info(f"Searching in default columns: {', '.join(search_cols)}")

    # Build WHERE clause
    conditions = [f"{col} ILIKE '%{query}%'" for col in search_cols]
    where_clause = " OR ".join(conditions)
    
    # Dynamic Select
    select_cols = ", ".join(all_cols)
    
    sql = f"""
        SELECT {select_cols}
        FROM vault.breach_records
        WHERE {where_clause}
        LIMIT {limit}
    """
    
    log_info(f"Executing Search: [bold]{query}[/bold] on {len(search_cols)} columns")
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
            # Handle potential None values for display
            safe_row = [str(r) if r is not None else "" for r in row]
            table.add_row(*safe_row)
            
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
        # Bypass 50GB drop limit by setting max_table_size_to_drop to 0 (unlimited)
        repo.client.command("TRUNCATE TABLE vault.breach_records", settings={'max_table_size_to_drop': 0})
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
        log_info("You can safely press 'Ctrl+C' to detach (the server will continue working), or wait for it to finish.")
        
        # Force merge to remove 'inactive' parts left by delete
        # Increase timeout to 1 hour (3600s) for heavy operations
        repo.client.command("OPTIMIZE TABLE vault.breach_records FINAL", settings={'receive_timeout': 3600})
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
    source_name: str = typer.Option(None, "--source-name", help="Custom name for the data source"),
    format: str = typer.Option("auto", help="Format hint: 'auto' or 'email:pass' (faster)"),
    no_check: bool = typer.Option(False, help="Disable email validation in Fast Path (Dangerous but Fastest)"),
    batch_size: int = typer.Option(None, help="Batch size (rows) per chunk. Defaults to config (50K)."),
    watch: bool = typer.Option(False, help="Watch raw directory for new files"),
    workers: int = typer.Option(1, "--workers", "-w", help="Number of concurrent upload workers. Defaults to 1."),
    append: bool = typer.Option(False, "--append", help="Append data to existing source file instead of overwriting")
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
        
        final_source_name = source_name or "stdin"
        ingestor.process_stream(
            sys.stdin, 
            settings.STAGING_DIR, 
            settings.QUARANTINE_DIR, 
            batch_size=final_batch_size, 
            source_name=final_source_name, 
            format=format, 
            no_check=no_check,
            num_workers=workers,
            append=append
        )
        return

    if file:
        ingestor.process_file(
            file, 
            settings.STAGING_DIR, 
            settings.QUARANTINE_DIR, 
            batch_size=final_batch_size, 
            format=format, 
            no_check=no_check, 
            custom_source_name=source_name,
            num_workers=workers,
            append=append
        )
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
                ingestor.process_file(
                    f, 
                    settings.STAGING_DIR, 
                    settings.QUARANTINE_DIR, 
                    batch_size=final_batch_size, 
                    format=format, 
                    no_check=no_check, 
                    custom_source_name=source_name,
                    num_workers=workers,
                    append=append
                )

@app.command()
def info(
    limit: int = typer.Option(50, help="Max source files to list")
):
    """Displays comprehensive statistics about the breach database."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.layout import Layout
    from rich import box

    repo = ClickHouseAdapter()
    console = Console()
    
    # Fetch Data
    try:
        stats = repo.get_table_stats("vault.breach_records")
        cols = repo.get_columns("vault.breach_records")
        indices = repo.get_indices("vault.breach_records")
        sources = repo.get_source_file_stats("vault.breach_records", limit)
    except Exception as e:
        log_error(f"Failed to fetch info: {e}")
        return

    # 1. Overview Panel
    overview_table = Table.grid(padding=1)
    overview_table.add_column(style="bold cyan", justify="right")
    overview_table.add_column(style="white")
    
    overview_table.add_row("Total Records:", f"{stats['total_rows']:,}")
    overview_table.add_row("Compressed Size:", str(stats['compressed_size']))
    overview_table.add_row("Uncompressed:", str(stats['uncompressed_size']))
    overview_table.add_row("Compression Ratio:", f"{stats['compression_ratio']}x")
    overview_table.add_row("Total Columns:", str(len(cols)))
    overview_table.add_row("Active Indices:", str(len(indices)))

    overview_panel = Panel(
        overview_table, 
        title="[bold blue]Database Overview[/bold blue]", 
        border_style="blue",
        box=box.ROUNDED
    )

    # 2. Schema Panel
    schema_text = ", ".join([f"[green]{c}[/green]" for c in cols])
    schema_panel = Panel(
        schema_text, 
        title="[bold green]Current Schema[/bold green]", 
        border_style="green",
        box=box.ROUNDED
    )
    
    # 3. Indices Panel
    idx_table = Table(box=box.SIMPLE_HEAD, expand=True)
    idx_table.add_column("Index Name", style="yellow")
    idx_table.add_column("Type")
    idx_table.add_column("Granularity")
    
    for idx in indices:
        idx_table.add_row(idx[0], idx[1], str(idx[3]))
        
    idx_panel = Panel(
        idx_table,
        title="[bold yellow]Skipping Indices[/bold yellow]",
        border_style="yellow",
        box=box.ROUNDED
    )

    # 4. Source Files Table
    src_table = Table(title=f"Top {limit} Source Files", box=box.MINIMAL_DOUBLE_HEAD)
    src_table.add_column("Source File", style="bold magenta")
    src_table.add_column("Rows", justify="right")
    src_table.add_column("First Import", style="dim")
    src_table.add_column("Last Import", style="dim")
    
    for src in sources:
        src_table.add_row(
            src[0], 
            f"{src[1]:,}", 
            str(src[2]), 
            str(src[3])
        )

    # Rendering
    console.print(overview_panel)
    console.print(schema_panel)
    if indices:
        console.print(idx_panel)
    console.print(src_table)

if __name__ == "__main__":
    app()
