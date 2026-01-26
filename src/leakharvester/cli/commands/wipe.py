import typer
from rich.prompt import Confirm
from leakharvester.adapters.console import log_info, log_error, log_success
from leakharvester.adapters.clickhouse import ClickHouseAdapter

def wipe_command(
    filenames: list[str] = typer.Argument(None, help="List of source filenames to wipe data for"),
    all: bool = typer.Option(False, "--all", help="Wipe ALL data (Truncate Table). Instant space reclamation.")
):
    """Wipes data associated with specific source files or all data."""
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
