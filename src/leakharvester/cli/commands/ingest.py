import typer
import sys
from pathlib import Path
from rich.prompt import Confirm
from leakharvester.config import settings
from leakharvester.adapters.console import log_info, log_error, log_warning
from leakharvester.adapters.clickhouse import ClickHouseAdapter
from leakharvester.adapters.local_fs import LocalFileSystemAdapter
from leakharvester.services.ingestor import BreachIngestor

def _confirm_schema_update(missing_cols: list[str]) -> bool:
    return Confirm.ask(f"Do you want to add these {len(missing_cols)} columns to the database schema?", default=True)

def ingest_command(
    file: Path = typer.Option(None, help="Specific file to ingest"),
    stdin: bool = typer.Option(False, "--stdin", help="Ingest from standard input (pipe)."),
    source_name: str = typer.Option(None, "--source-name", help="Custom name for the data source."),
    format: str = typer.Option("auto", help="Input format. Use 'auto' for detection. Specify 'col1:col2' (e.g. 'email:password') to skip detection (Faster startup)."),
    skip_email_validation: bool = typer.Option(False, "--unsafe", help="Disable email validation in Fast Path (Dangerous but Fastest)."),
    batch_size: int = typer.Option(None, help="Batch size (rows) per chunk. Defaults to config (1M)."),
    watch: bool = typer.Option(False, help="Watch raw directory for new files."),
    workers: int = typer.Option(4, "--workers", "-w", help="Number of concurrent upload workers. Defaults to 4."),
    append: bool = typer.Option(False, "--append", help="Append data to existing source file instead of overwriting.")
):
    """Ingests data from raw directory, specific file, or stdin pipe."""
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
            skip_email_validation=skip_email_validation,
            num_workers=workers,
            append=append,
            on_schema_mismatch=_confirm_schema_update
        )
        return

    if file:
        ingestor.process_file(
            file, 
            settings.STAGING_DIR, 
            settings.QUARANTINE_DIR, 
            batch_size=final_batch_size, 
            format=format, 
            skip_email_validation=skip_email_validation, 
            custom_source_name=source_name,
            num_workers=workers,
            append=append,
            on_schema_mismatch=_confirm_schema_update
        )
    else:
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
                    skip_email_validation=skip_email_validation, 
                    custom_source_name=source_name,
                    num_workers=workers,
                    append=append,
                    on_schema_mismatch=_confirm_schema_update
                )
