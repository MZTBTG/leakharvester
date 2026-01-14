import typer
from pathlib import Path
from rich.prompt import Prompt, Confirm
from rich.console import Console
from rich.table import Table
from leakharvester.config import settings
from leakharvester.adapters.console import log_info, log_success, log_error, log_warning
from typing import TYPE_CHECKING
import itertools

if TYPE_CHECKING:
    import pyarrow as pa

app = typer.Typer(name="leakharvester", help="High-Performance Breach Data Ingestion Engine", rich_markup_mode="rich")

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
    old_parts_lifetime = 60;
"""

@app.command()
def init_db(
    reset: bool = typer.Option(False, "--reset", help="DROP existing table and recreate schema ([bold red]Data Loss![/bold red])")
):
    """Initializes the ClickHouse database and tables (without heavy indexes)."""
    from leakharvester.adapters.clickhouse import ClickHouseAdapter
    try:
        settings.create_dirs()
        repo = ClickHouseAdapter()
        
        if reset:
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
def index(
    # Targeting
    column: str = typer.Option(None, "-c", "--column", help="Target specific columns (comma-separated)."),
    remove: bool = typer.Option(False, "-r", "--remove", help="Drop existing indexes on targeted columns."),
    list_indexes: bool = typer.Option(False, "-l", "--list", help="List active indexes."),
    
    # Automation
    auto_optimize: bool = typer.Option(False, "-a", "--auto-optimize", help="Run [bold green]Heuristic Analyzer[/bold green] to automatically recommend indexes."),
    auto_random: bool = typer.Option(False, "--auto-random", help="Use random sampling for analyzer (Slower but unbiased)."),

    # Manual Overrides
    tokenbf: bool = typer.Option(False, "-t", "--tokenbf", help="Apply [cyan]Token Bloom Filter[/cyan]."),
    tokenbf_size: int = typer.Option(32768, "--tokenbf-size", help="Bloom filter size in bytes."),
    tokenbf_hash: int = typer.Option(3, "--tokenbf-hash", help="Number of hash functions."),
    tokenbf_seed: int = typer.Option(0, "--tokenbf-seed", help="Random seed."),
    
    ngram: bool = typer.Option(False, "-n", "--ngram", help="Apply [cyan]N-Gram Bloom Filter[/cyan]."),
    ngram_n: int = typer.Option(4, "--ngram-n", help="Gram size (e.g. 4 for trigrams)."),
    ngram_size: int = typer.Option(32768, "--ngram-size", help="Bloom filter size in bytes."),
    
    inverted: bool = typer.Option(False, "-i", "--inverted", help="Apply [cyan]Inverted Index[/cyan] (ClickHouse native)."),
):
    """
    Intelligent Database Indexing Orchestrator.
    Manages performance indexes using Heuristic Analysis or Manual Configuration.

    [bold]INDEX TYPES REFERENCE:[/bold]

    [cyan]1. Inverted Index (Log/Inverted)[/cyan]
    [dim]Technical:[/dim] Maps terms to row IDs (Segment postings). ClickHouse `inverted(0)`.
    [green]Pros:[/green] Excellent for full-text search, high cardinality exact matches, and `hasToken` queries.
    [red]Cons:[/red] High storage overhead for unique text.

    [cyan]2. Token Bloom Filter (TokenBF)[/cyan]
    [dim]Technical:[/dim] Splits string into tokens (by non-alphanumeric), hashes them into a Bloom Filter.
    [green]Pros:[/green] Very small storage. Good for checking if a token exists in a block.
    [red]Cons:[/red] False positives possible. Cannot find substrings *inside* tokens (e.g. 'pass' in 'password').

    [cyan]3. N-Gram Bloom Filter (NgramBF)[/cyan]
    [dim]Technical:[/dim] Splits string into N-sized grams (sliding window).
    [green]Pros:[/green] Finds arbitrary substrings (e.g., '123' in 'password123').
    [red]Cons:[/red] Larger storage than TokenBF. Computationally expensive to build.

    [bold]EXAMPLES:[/bold]
      [yellow]leakharvester index --auto-optimize[/yellow] (Recommended)
      [yellow]leakharvester index -c email -i[/yellow] (Manual Inverted Index on Email)
      [yellow]leakharvester index -c password -n --ngram-n 3[/yellow] (Trigram index on Password)
    """
    from leakharvester.adapters.clickhouse import ClickHouseAdapter
    from leakharvester.services.index_optimizer import IndexManager, HeuristicAnalyzer
    repo = ClickHouseAdapter()
    manager = IndexManager(repo)
    console = Console()
    
    # 1. LIST MODE
    if list_indexes:
        idxs = manager.list_indexes()
        table = Table(title="Active Indexes")
        table.add_column("Name", style="cyan")
        table.add_column("Column", style="magenta")
        table.add_column("Type", style="green")
        table.add_column("Granularity")
        
        for i in idxs:
            table.add_row(i['name'], i['column'], i['type'], str(i['granularity']))
        console.print(table)
        return

    # 2. TARGET VALIDATION
    target_cols = []
    if column:
        # Validate columns
        all_cols = repo.get_columns("vault.breach_records")
        requested = [c.strip() for c in column.split(",")]
        invalid = [c for c in requested if c not in all_cols]
        if invalid:
            log_error(f"Invalid columns: {invalid}")
            console.print(f"Valid columns: {', '.join(all_cols)}")
            raise typer.Exit(1)
        target_cols = requested
    elif auto_optimize:
        # Auto targets all String columns if not specified
        target_cols = repo.get_columns("vault.breach_records")
    else:
        # If no column and no list/auto, show help or error
        if not remove: 
             log_error("Please specify columns (-c) or enable automation (-a) or list (-l).")
             raise typer.Exit(1)

    # 3. REMOVE MODE
    if remove:
        if not column:
            log_error("Please specify columns to remove indexes from.")
            raise typer.Exit(1)
        
        if Confirm.ask(f"Drop indexes for {target_cols}?"):
            for col in target_cols:
                manager.drop_index(col)
        return

    # 4. MANUAL CONFIGURATION
    if tokenbf or ngram or inverted:
        if not column:
            log_error("Manual index configuration requires target columns (-c).")
            raise typer.Exit(1)
            
        ddl = ""
        if inverted:
            ddl = "TYPE inverted(0) GRANULARITY 1"
        elif tokenbf:
            ddl = f"TYPE tokenbf_v1({tokenbf_size}, {tokenbf_hash}, {tokenbf_seed}) GRANULARITY 1"
        elif ngram:
            ddl = f"TYPE ngrambf_v1({ngram_n}, {ngram_size}, 2, 0) GRANULARITY 1"
            
        for col in target_cols:
            manager.apply_index(col, ddl)
        return

    # 5. AUTO OPTIMIZATION (Interactive)
    if auto_optimize:
        analyzer = HeuristicAnalyzer(repo)
        
        for col in target_cols:
            sample_size = 10000
            while True:
                console.print(f"\n[bold]Analyzing column: {col}[/bold]")
                rec = analyzer.analyze_column("vault.breach_records", col, sample_size, auto_random)
                
                # Render Report
                rtable = Table(show_header=False, box=None)
                rtable.add_row("Recommendation:", f"[{'green' if rec.confidence > 0.8 else 'yellow'}]{rec.type}[/]")
                rtable.add_row("Confidence:", f"{rec.confidence:.0%}")
                rtable.add_row("Reason:", rec.reason)
                console.print(rtable)
                
                if rec.type == "NONE" and rec.confidence > 0.9:
                    log_info(f"Skipping {col} (No index recommended).")
                    break

                # Prompt
                menu_legend = """
  [bold cyan][A][/bold cyan]ccept Recommendation (Default)
  [bold cyan][S][/bold cyan]elect Manual Index
  [bold cyan][D][/bold cyan]eep Analyze (Increase Sample Size)
  S[bold cyan][K][/bold cyan]ip Column
  [bold cyan][Q][/bold cyan]uit
"""
                console.print(menu_legend)
                
                choice = Prompt.ask(
                    "Action", 
                    choices=["a", "s", "d", "k", "q"], 
                    default="a"
                ).lower()
                
                if choice == "q":
                    raise typer.Exit()
                elif choice == "k":
                    log_info("Skipped.")
                    break
                elif choice == "a":
                    if rec.type != "NONE":
                        manager.apply_index(col, rec.ddl_params)
                    break
                elif choice == "d":
                    sample_size *= 2
                    log_info(f"Deep analyzing with {sample_size} rows...")
                    continue # Re-loop
                elif choice == "s":
                    # Simple manual override selection
                    sel = Prompt.ask("Select Type", choices=["inverted", "tokenbf", "ngram", "none"])
                    if sel == "none": break
                    
                    # Defaults for manual override via this menu
                    if sel == "inverted": ddl = "TYPE inverted(0) GRANULARITY 1"
                    elif sel == "tokenbf": ddl = "TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1"
                    elif sel == "ngram": ddl = "TYPE ngrambf_v1(4, 32768, 2, 0) GRANULARITY 1"
                    
                    manager.apply_index(col, ddl)
                    break

@app.command()
def search(
    query: str = typer.Argument(None, help="Search term (e.g. 'augusto.bachini', 'password123')"),
    limit: int = typer.Option(20, help="Max results to display"),
    column: str = typer.Option(None, help="Comma-separated list of columns to search (default: all)"),
    list_columns: bool = typer.Option(False, "--list-columns", help="List available columns (Introspection)"),
):
    """Searches the breach database using ILIKE across all string columns."""
    from rich.table import Table
    from rich.console import Console
    from rich.panel import Panel
    from rich import box
    import time
    from leakharvester.adapters.clickhouse import ClickHouseAdapter
    
    console = Console()
    repo = ClickHouseAdapter()
    
    # Fetch existing columns dynamically
    try:
        all_cols = repo.get_columns("vault.breach_records")
    except Exception as e:
        log_error(f"Failed to fetch table schema: {e}")
        return

    # Handle introspection request
    if list_columns:
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
        search_cols = [c for c in all_cols if c not in ('breach_date', 'import_date')]
        log_info(f"Searching in default columns: {', '.join(search_cols)}")

    # Build WHERE clause
    conditions = [f"{col} ILIKE '%{query}%'" for col in search_cols]
    where_clause = " OR ".join(conditions)
    
    # Check for Index Coverage (Warning)
    try:
        indices = repo.get_indices("vault.breach_records")
        # Map of column_name -> has_optimized_index
        # We consider 'inverted', 'tokenbf', and 'ngrambf' as valid optimizations for text search
        optimized_cols = set()
        
        for idx in indices:
            name, type_def = idx[0], idx[1].lower()
            target_col = None
            
            # Heuristic to extract column name from index name (idx_{col} or idx_{col}_{type})
            # Or reliance on 'expr' from get_indices if available (it is idx[2])
            # Let's try to parse the expression or name more robustly
            expr = idx[2] if len(idx) > 2 else ""
            
            # If expr is simple column name
            if expr in all_cols:
                target_col = expr
            # Fallback to name parsing if expr is complex or unavailable
            elif name.startswith("idx_"):
                parts = name.split("_")
                # Try finding which part matches a known column
                for col in all_cols:
                    if col in parts:
                        target_col = col
                        break
            
            if target_col and any(t in type_def for t in ("inverted", "tokenbf", "ngrambf")):
                optimized_cols.add(target_col)
        
        unindexed = [c for c in search_cols if c not in optimized_cols]
        if unindexed:
            console.print(f"[yellow]⚠ Warning: Full Table Scan detected on columns: {unindexed}[/yellow]")
            console.print("[dim]  Performance may be slow. Run 'leakharvester index --auto-optimize' to index them.[/dim]")
            
    except Exception:
        pass 
    
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
    from leakharvester.adapters.clickhouse import ClickHouseAdapter
    repo = ClickHouseAdapter()
    
    if all:
        if not Confirm.ask("[bold red]DANGER:[/bold red] This will TRUNCATE the entire database. All data will be lost instantly. Are you sure?"):
            log_info("Wipe operation cancelled.")
            return
        
        log_info("Executing TRUNCATE TABLE (Nuclear Option)...")
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
        repo.client.command("OPTIMIZE TABLE vault.breach_records FINAL", settings={'receive_timeout': 3600})
        log_success("Optimization complete. Disk space reclaimed.")
        
    except Exception as e:
        log_error(f"Wipe operation failed: {e}")

@app.command()
def repair():
    """Attempts to repair and ingest data from the quarantine directory."""
    from leakharvester.adapters.clickhouse import ClickHouseAdapter
    from leakharvester.adapters.local_fs import LocalFileSystemAdapter
    from leakharvester.services.ingestor import BreachIngestor
    repo = ClickHouseAdapter()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    ingestor.repair_quarantine(settings.QUARANTINE_DIR, settings.STAGING_DIR)

@app.command()
def ingest(
    file: Path = typer.Option(None, help="Specific file to ingest"),
    stdin: bool = typer.Option(False, "--stdin", help="Ingest from standard input (pipe)."),
    source_name: str = typer.Option(None, "--source-name", help="Custom name for the data source."),
    format: str = typer.Option("auto", help="Input format. Use 'auto' for detection. Specify 'col1:col2' (e.g. 'email:password') to skip detection (Faster startup)."),
    no_check: bool = typer.Option(False, "--unsafe", help="Disable email validation in Fast Path (Dangerous but Fastest)."),
    batch_size: int = typer.Option(None, help="Batch size (rows) per chunk. Defaults to config (50K)."),
    watch: bool = typer.Option(False, help="Watch raw directory for new files."),
    workers: int = typer.Option(1, "--workers", "-w", help="Number of concurrent upload workers. Defaults to 1."),
    append: bool = typer.Option(False, "--append", help="Append data to existing source file instead of overwriting.")
):
    """Ingests data from raw directory, specific file, or stdin pipe."""
    import sys
    from leakharvester.adapters.clickhouse import ClickHouseAdapter
    from leakharvester.adapters.local_fs import LocalFileSystemAdapter
    from leakharvester.services.ingestor import BreachIngestor
    
    final_batch_size = batch_size or settings.BATCH_SIZE
    
    repo = ClickHouseAdapter()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    if stdin:
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
        files = list(settings.RAW_DIR.glob("*\n"))
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
    limit: int = typer.Option(50, help="Max source files to list.")
):
    """Displays comprehensive statistics about the breach database."""
    from rich.panel import Panel
    from rich import box
    from leakharvester.adapters.clickhouse import ClickHouseAdapter

    repo = ClickHouseAdapter()
    console = Console()
    
    try:
        stats = repo.get_table_stats("vault.breach_records")
        cols = repo.get_columns("vault.breach_records")
        indices = repo.get_indices("vault.breach_records")
        sources = repo.get_source_file_stats("vault.breach_records", limit)
    except Exception as e:
        log_error(f"Failed to fetch info: {e}")
        return

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

    schema_text = ", ".join([f"[green]{c}[/green]" for c in cols])
    schema_panel = Panel(
        schema_text, 
        title="[bold green]Current Schema[/bold green]", 
        border_style="green",
        box=box.ROUNDED
    )
    
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

    console.print(overview_panel)
    console.print(schema_panel)
    if indices:
        console.print(idx_panel)
    console.print(src_table)

@app.command(name="export")
def export_data(
    output: Path = typer.Option(..., "-o", "--output", help="Destination .lh file"),
    compression_level: int = typer.Option(3, "-l", "--compression-level", help="ZSTD Level (1-19)"),
    include_columns: str = typer.Option(None, "-c", "--include-columns", help="Whitelist columns (comma-separated)"),
    exclude_columns: str = typer.Option(None, "-e", "--exclude-columns", help="Blacklist columns (comma-separated)"),
    no_pass: bool = typer.Option(False, "-p", "--no-pass", help="Disable encryption (Plaintext compressed)"),
    no_index: bool = typer.Option(False, "--no-index", help="Exclude index reconstruction data (Not implemented, placeholder)")
):
    """Exports data to a secure, compressed .lh container."""
    from leakharvester.adapters.clickhouse import ClickHouseAdapter
    from leakharvester.services.secure_io import SecureIO
    repo = ClickHouseAdapter()
    
    all_cols = repo.get_columns("vault.breach_records")
    final_cols = all_cols
    
    if include_columns:
        whitelist = [c.strip() for c in include_columns.split(",")]
        final_cols = [c for c in all_cols if c in whitelist]
    elif exclude_columns:
        blacklist = [c.strip() for c in exclude_columns.split(",")]
        final_cols = [c for c in all_cols if c not in blacklist]
        
    if not final_cols:
        log_error("No columns selected for export.")
        raise typer.Exit(code=1)
        
    log_info(f"Exporting columns: {', '.join(final_cols)}")
    
    password = None
    if not no_pass:
        password = typer.prompt("Enter encryption password", hide_input=True, confirmation_prompt=True)
    
    query = f"SELECT {', '.join(final_cols)} FROM vault.breach_records"
    
    try:
        with repo.client.query_arrow_stream(query) as stream:
             log_info(f"Streaming data to {output} (Encrypted: {not no_pass}, ZSTD: {compression_level})...")

             # Defensive: some clients return a RecordBatchReader with `.schema`,
             # others return a generator/iterator of RecordBatches (no `.schema`).
             schema = getattr(stream, "schema", None)
             if schema is None:
                 # Attempt to pull the first RecordBatch to determine schema,
                 # then chain it back with the original iterator so no data is lost.
                 try:
                     first_batch = next(stream)
                 except StopIteration:
                     log_error("Export failed: query returned no data to export.")
                     raise typer.Exit(code=1)
                 schema = first_batch.schema
                 arrow_iter = itertools.chain([first_batch], stream)
             else:
                 arrow_iter = stream

             SecureIO.export_data(
                 output_path=output,
                 arrow_stream=arrow_iter,
                 schema=schema,
                 password=password,
                 compression_level=compression_level
             )
        
        log_success(f"Export complete: {output}")
        
    except Exception as e:
        log_error(f"Export failed: {e}")
        if output.exists():
            output.unlink()
        raise typer.Exit(code=1)

@app.command(name="import")
def import_data(
    input_file: Path = typer.Option(..., "-i", "--input-file", help="Source .lh file"),
    include_columns: str = typer.Option(None, "-c", "--include-columns", help="Whitelist columns to import"),
    exclude_columns: str = typer.Option(None, "-e", "--exclude-columns", help="Blacklist columns from import"),
    no_index: bool = typer.Option(False, "--no-index", help="Skip index rebuilding after import (Faster)"),
):
    """Imports data from a secure .lh container."""
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
    import time
    from leakharvester.adapters.clickhouse import ClickHouseAdapter
    from leakharvester.services.secure_io import SecureIO

    repo = ClickHouseAdapter()
    
    password = None
    try:
        SecureIO.import_data(input_file, password=None)
    except Exception as e:
        if "Password required" in str(e):
             password = typer.prompt("Enter decryption password", hide_input=True)
    
    try:
        arrow_stream = SecureIO.import_data(input_file, password=password)
    except Exception as e:
        log_error(f"Import init failed: {e}")
        raise typer.Exit(code=1)

    log_info(f"Importing from {input_file}...")
    
    total_rows = 0
    start_time = time.time()
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=False
        ) as progress:
            task = progress.add_task("[cyan]Importing Batches...", total=None)
            
            for batch in arrow_stream:
                if include_columns or exclude_columns:
                    current_cols = batch.schema.names
                    selected_cols = current_cols
                    
                    if include_columns:
                        whitelist = [c.strip() for c in include_columns.split(",")]
                        selected_cols = [c for c in current_cols if c in whitelist]
                    elif exclude_columns:
                        blacklist = [c.strip() for c in exclude_columns.split(",")]
                        selected_cols = [c for c in current_cols if c not in blacklist]
                    
                    try:
                        indices = [batch.schema.get_field_index(c) for c in selected_cols if c in current_cols]
                        batch = batch.select(indices)
                    except Exception as filter_err:
                        log_warning(f"Column filtering failed for batch, skipping filter: {filter_err}")

                rows = batch.num_rows
                total_rows += rows
                repo.insert_arrow_batch(batch, "vault.breach_records")
                progress.update(task, advance=rows, description=f"[cyan]Imported {total_rows:,} rows...")
        
        elapsed = time.time() - start_time
        log_success(f"Import complete. Total rows: {total_rows:,} in {elapsed:.2f}s")
        
        if not no_index:
            log_info("Triggering background merge (OPTIMIZE)...")
            repo.client.command("OPTIMIZE TABLE vault.breach_records")
            
    except Exception as e:
        log_error(f"Import failed: {e}")
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
