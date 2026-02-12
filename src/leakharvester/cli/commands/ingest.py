import typer
import sys
from pathlib import Path
from rich.prompt import Confirm
from rich.console import Console
from rich.panel import Panel
from leakharvester.config import settings
from leakharvester.adapters.console import log_info, log_error, log_warning
from leakharvester.adapters.clickhouse import ClickHouseAdapter
from leakharvester.adapters.local_fs import LocalFileSystemAdapter
from leakharvester.services.ingestor import BreachIngestor
from leakharvester.domain.exceptions import AmbiguousFormatException

def _confirm_schema_update(missing_cols: list[str]) -> bool:
    return Confirm.ask(f"Do you want to add these {len(missing_cols)} columns to the database schema?", default=True)

def _handle_ambiguous_format(e: AmbiguousFormatException):
    console = Console()
    config = e.config
    cols = e.columns
    input_path = e.input_path
    fmt_hint = config.get("separator", ":").join(cols)
    
    msg = f"""
[bold yellow]Ambiguous File Structure Detected[/bold yellow]
Auto-ingestion paused to prevent data corruption.

[bold]Detected Config:[/bold] Sep='{config.get("separator")}' Header={config.get("has_header")}
[bold]Mapped Columns:[/bold] {cols}

[bold green]Suggested Command:[/bold green]
leakharvester ingest --file "{input_path}" --format "{fmt_hint}"

[dim]Replace 'unknown' with standard names (username, ip, etc) or 'null'.[/dim]
    """
    console.print(Panel(msg, title="Format Suggestion", border_style="yellow"))

def ingest_command(
    file: Path = typer.Option(None, help="Specific file to ingest"),
    stdin: bool = typer.Option(False, "--stdin", help="Ingest from standard input (pipe)."),
    source_name: str = typer.Option(None, "--source-name", help="Custom name for the data source."),
    format: str = typer.Option("auto", help="Specify column schema (e.g. 'email:password') or use 'auto' for auto-detection."),
    skip_email_validation: bool = typer.Option(False, "--unsafe", help="Disable email validation in Fast Path (Dangerous but Fastest)."),
    batch_size: int = typer.Option(None, help="Batch size (rows) per chunk. Defaults to config (1M)."),
    watch: bool = typer.Option(False, help="Watch raw directory for new files."),
    workers: int = typer.Option(4, "--workers", "-w", help="Number of concurrent upload workers. Defaults to 4."),
    append: bool = typer.Option(False, "--append", help="Append data to existing source file instead of overwriting.")
):
    """Ingests data from raw directory, specific file, or stdin pipe."""
    final_batch_size = batch_size or settings.BATCH_SIZE
    
    from leakharvester.settings_manager import SettingsManager
    sm = SettingsManager()
    db_path = sm.get_active_db_path()
    
    staging_dir = settings.STAGING_DIR
    quarantine_dir = settings.QUARANTINE_DIR
    
    if db_path:
        staging_dir = db_path / "staging"
        quarantine_dir = db_path / "quarantine"
        
        # Ensure directories exist
        staging_dir.mkdir(parents=True, exist_ok=True)
        quarantine_dir.mkdir(parents=True, exist_ok=True)
    
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
            staging_dir, 
            quarantine_dir, 
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
        try:
            ingestor.process_file(
                file, 
                staging_dir, 
                quarantine_dir, 
                batch_size=final_batch_size, 
                format=format, 
                skip_email_validation=skip_email_validation, 
                custom_source_name=source_name,
                num_workers=workers,
                append=append,
                on_schema_mismatch=_confirm_schema_update
            )
        except AmbiguousFormatException as e:
            _handle_ambiguous_format(e)
    else:
        files = list(settings.RAW_DIR.glob("*"))
        if not files:
            log_info("No files found in raw directory.")
            return

        for f in files:
            if f.is_file():
                try:
                    ingestor.process_file(
                        f, 
                        staging_dir, 
                        quarantine_dir, 
                        batch_size=final_batch_size, 
                        format=format, 
                        skip_email_validation=skip_email_validation, 
                        custom_source_name=source_name,
                        num_workers=workers,
                        append=append,
                        on_schema_mismatch=_confirm_schema_update
                    )
                except AmbiguousFormatException as e:
                    _handle_ambiguous_format(e)
