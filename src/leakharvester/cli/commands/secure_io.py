import typer
import itertools
from pathlib import Path
from rich.progress import Progress, SpinnerColumn, TextColumn
import time
from leakharvester.adapters.console import log_info, log_error, log_success, log_warning
from leakharvester.adapters.clickhouse import ClickHouseAdapter
from leakharvester.services.secure_io import SecureIO

def export_command(
    output: Path = typer.Option(..., "-o", "--output", help="Destination .lh file"),
    compression_level: int = typer.Option(3, "-l", "--compression-level", help="ZSTD Level (1-19)"),
    include_columns: str = typer.Option(None, "-c", "--include-columns", help="Whitelist columns (comma-separated)"),
    exclude_columns: str = typer.Option(None, "-e", "--exclude-columns", help="Blacklist columns (comma-separated)"),
    no_pass: bool = typer.Option(False, "-p", "--no-pass", help="Disable encryption (Plaintext compressed)"),
    no_index: bool = typer.Option(False, "--no-index", help="Exclude index reconstruction data (Not implemented, placeholder)")
):
    """Exports data to a secure, compressed .lh container."""
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

             schema = getattr(stream, "schema", None)
             if schema is None:
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

def import_command(
    input_file: Path = typer.Option(..., "-i", "--input-file", help="Source .lh file"),
    include_columns: str = typer.Option(None, "-c", "--include-columns", help="Whitelist columns to import"),
    exclude_columns: str = typer.Option(None, "-e", "--exclude-columns", help="Blacklist columns from import"),
    no_index: bool = typer.Option(False, "--no-index", help="Skip index rebuilding after import (Faster)"),
):
    """Imports data from a secure .lh container."""
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
