import typer
from pathlib import Path
from rich.prompt import Prompt, Confirm
from rich.table import Table
from rich.console import Console
from leakharvester.config import settings
from leakharvester.adapters.console import log_info, log_success, log_error, log_warning
from leakharvester.settings_manager import SettingsManager
from leakharvester.adapters.clickhouse import ClickHouseAdapter
import urllib.request
import os
import shutil
import subprocess
import time
import uuid

def ensure_db_running(force_restart: bool = False):
    host = settings.CLICKHOUSE_HOST or "localhost"
    port = settings.CLICKHOUSE_PORT or 8123
    ping_url = f"http://{host}:{port}/ping"

    def is_online():
        try:
            with urllib.request.urlopen(ping_url, timeout=1) as response:
                return response.read().strip() == b"Ok."
        except Exception:
            return False

    if not force_restart and is_online():
        return

    if force_restart:
        log_warning("Forcing database restart to apply configuration changes...")

    sm = SettingsManager()
    active_path = sm.get_active_db_path()
    
    env = os.environ.copy()
    if active_path:
        env["DB_VOLUME_PATH"] = str(active_path.resolve())
    else:
        env["DB_VOLUME_PATH"] = "./data/clickhouse_data"

    docker_cmd = None
    if shutil.which("docker"):
        try:
            subprocess.run(["docker", "compose", "version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            docker_cmd = ["docker", "compose"]
        except subprocess.CalledProcessError:
            pass
    
    if not docker_cmd and shutil.which("docker-compose"):
        docker_cmd = ["docker-compose"]

    if not docker_cmd:
        log_warning("Docker or Docker Compose not found. Cannot auto-start database.")
        log_info("Please ensure ClickHouse is running manually at localhost:8123.")
        return
    
    if not (Path("docker-compose.yml").exists() or Path("compose.yaml").exists()):
        log_warning("No docker-compose.yml found. Cannot auto-start database.")
        return

    if force_restart:
        log_info("Stopping existing container...")
        subprocess.run(docker_cmd + ["down"], check=False, env=env)

    log_info(f"Starting ClickHouse via Docker (Volume: {env['DB_VOLUME_PATH']})...")
    
    up_args = ["up", "-d"]
    if force_restart:
        up_args.append("--force-recreate")
    up_args.append("clickhouse")

    try:
        subprocess.run(docker_cmd + up_args, check=True, env=env, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        err_msg = e.stderr.lower()
        if "permission denied" in err_msg and "docker.sock" in err_msg:
            log_error("Docker Permission Denied.")
            log_info("Your user is not in the 'docker' group.")
            log_info("Run this command to fix it:")
            Console().print(f"[bold green]sudo usermod -aG docker {os.environ.get('USER', '$USER')} && newgrp docker[/bold green]")
        else:
            log_error(f"Failed to start Docker container: {e.stderr}")
        raise typer.Exit(1)

    log_info("Waiting for database to initialize...")
    for i in range(60):
        if is_online():
            log_success("Database came online!")
            return
        time.sleep(1)
    
    log_error("Database failed to respond after 60 seconds.")
    raise typer.Exit(1)

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

CREATE TABLE IF NOT EXISTS vault.system_info
(
    `key` String,
    `value` String
)
ENGINE = TinyLog;
"""

def db_command(
    path: Path = typer.Option(None, "--path", "-p", help="Set or query the active database path."),
    init: bool = typer.Option(False, "--init", help="Initialize the database at the configured path."),
    status: bool = typer.Option(False, "--status", "-s", help="Check database and Docker status."),
    lsfiles: bool = typer.Option(False, "--lsfiles", help="List all ingested source files."),
    rmfile: str = typer.Option(None, "--rmfile", help="Remove specific files (comma-separated)."),
    allfiles: bool = typer.Option(False, "--allfiles", help="Wipe ALL data (Truncate Table). Instant space reclamation."),
    remove: bool = typer.Option(False, "--remove", "-r", help="Remove the active database data (Stop & Delete)."),
    reset_all: bool = typer.Option(False, "--reset-all", help="FACTORY RESET: Wipes Config, Data, and Docker containers.")
):
    sm = SettingsManager()
    console = Console()

    if path:
        if path.exists():
            if not path.is_dir():
                log_error(f"Path {path} is not a directory.")
                raise typer.Exit(1)
            
            if any(path.iterdir()):
                log_info(f"Directory {path} is not empty.")
            else:
                log_info(f"Directory {path} is empty.")
            
            if Confirm.ask(f"Set active DB path to [cyan]{path.resolve()}[/cyan]?"):
                sm.set_active_db_path(path)
                log_success("Active database path updated.")
        else:
            if Confirm.ask(f"Path {path} does not exist. Create and set as DB path?"):
                path.mkdir(parents=True, exist_ok=True)
                sm.set_active_db_path(path)
                log_success(f"Created and set active path: {path.resolve()}")
        return

    if status:
        active_path = sm.get_active_db_path()
        console.print(f"[bold]Active Configured Path:[/bold] {active_path or '[dim]Not configured (Using defaults)[/dim]'}")
        
        ensure_db_running()
        try:
            repo = ClickHouseAdapter()
            stats = repo.get_table_stats("vault.breach_records")
            console.print("[green]Database is Online.[/green]")
            console.print(f"Records: {stats['total_rows']:,}")
        except Exception as e:
            console.print(f"[red]Database connection failed:[/red] {e}")
        return

    if lsfiles:
        ensure_db_running()
        try:
            repo = ClickHouseAdapter()
            query = """
                SELECT 
                    source_file,
                    count() as row_count,
                    min(import_date) as first_import,
                    max(import_date) as last_import
                FROM vault.breach_records
                GROUP BY source_file
                ORDER BY last_import DESC
            """
            result = repo.client.query(query)
            
            if not result.result_rows:
                console.print("[yellow]No files found in database.[/yellow]")
                return

            table = Table(title="Ingested Files Registry")
            table.add_column("Source File", style="cyan", no_wrap=True)
            table.add_column("Row Count", justify="right", style="magenta")
            table.add_column("First Import", justify="right", style="green")
            table.add_column("Last Import", justify="right", style="green")

            for row in result.result_rows:
                table.add_row(
                    row[0],
                    f"{row[1]:,}",
                    str(row[2]),
                    str(row[3])
                )
            
            console.print(table)
        except Exception as e:
            log_error(f"Failed to list files: {e}")
        return

    if rmfile:
        ensure_db_running()
        filenames = [f.strip() for f in rmfile.split(",") if f.strip()]
        if not filenames:
            log_error("No valid filenames provided.")
            return

        try:
            repo = ClickHouseAdapter()
            
            valid_files_result = repo.client.query("SELECT DISTINCT source_file FROM vault.breach_records")
            valid_files = {row[0] for row in valid_files_result.result_rows}
            
            invalid_files = [f for f in filenames if f not in valid_files]
            
            if invalid_files:
                log_error(f"Error: File(s) not found: {', '.join(invalid_files)}")
                log_info("Showing available files...")
                query = """
                    SELECT 
                        source_file,
                        count() as row_count,
                        min(import_date) as first_import,
                        max(import_date) as last_import
                    FROM vault.breach_records
                    GROUP BY source_file
                    ORDER BY last_import DESC
                """
                result = repo.client.query(query)
                if result.result_rows:
                    table = Table(title="Ingested Files Registry (Valid Options)")
                    table.add_column("Source File", style="cyan", no_wrap=True)
                    table.add_column("Row Count", justify="right", style="magenta")
                    table.add_column("First Import", justify="right", style="green")
                    table.add_column("Last Import", justify="right", style="green")
                    for row in result.result_rows:
                        table.add_row(row[0], f"{row[1]:,}", str(row[2]), str(row[3]))
                    console.print(table)
                return

            if Confirm.ask(f"Are you sure you want to delete data for {len(filenames)} file(s)?"):
                files_str = "', '".join(filenames)
                log_info(f"Deleting data for: {filenames}...")
                
                delete_sql = f"ALTER TABLE vault.breach_records DELETE WHERE source_file IN ('{files_str}')"
                repo.client.command(delete_sql)
                log_success("Delete mutation submitted.")
                
                log_info("Triggering OPTIMIZE TABLE FINAL to force physical disk cleanup...")
                repo.client.command("OPTIMIZE TABLE vault.breach_records FINAL", settings={'receive_timeout': 3600})
                log_success("Optimization complete.")

        except Exception as e:
            log_error(f"Failed to remove files: {e}")
        return

    if allfiles:
        ensure_db_running()
        console.print("[bold red]DANGER: This will TRUNCATE the entire database. All data will be lost instantly.[/bold red]")
        
        confirmation = Prompt.ask("Type 'wipe' to confirm total data deletion")
        if confirmation != "wipe":
            log_info("Operation aborted.")
            return

        try:
            repo = ClickHouseAdapter()
            log_info("Executing TRUNCATE TABLE (Nuclear Option)...")
            repo.client.command("TRUNCATE TABLE vault.breach_records", settings={'max_table_size_to_drop': 0})
            log_success("Database truncated. Disk space should be reclaimed immediately.")
        except Exception as e:
            log_error(f"Failed to truncate database: {e}")
        return

    if init:
        ensure_db_running(force_restart=True)
        try:
            settings.create_dirs()
            repo = ClickHouseAdapter()
            
            statements = DDL_SQL.split(";")
            for statement in statements:
                if statement.strip():
                    repo.execute_ddl(statement)
            
            # Generate and Store Instance ID
            instance_id = str(uuid.uuid4())
            sm.set_instance_id(instance_id)
            log_info(f"Generated Instance ID: {instance_id}")
            
            # Store ID in DB
            repo.client.command("INSERT INTO vault.system_info (key, value) VALUES", [('instance_id', instance_id)])
            
            log_success(f"Database initialized at {sm.get_active_db_path() or 'default location'}.")
        except Exception as e:
            log_error(f"Failed to initialize DB: {e}")
            raise typer.Exit(code=1)
        return

    if remove:
        active_path = sm.get_active_db_path()
        if not active_path:
            log_error("No active DB path configured. Cannot safely remove default data without explicit path.")
            return

        if Confirm.ask(f"[bold red]DANGER:[/bold red] Stop DB and DELETE all data at {active_path}?"):
            subprocess.run(["docker", "compose", "down"], check=False)
            try:
                shutil.rmtree(active_path)
                log_success(f"Deleted {active_path}")
            except Exception as e:
                if isinstance(e, PermissionError) or (hasattr(e, 'errno') and e.errno == 13):
                    log_warning("Permission denied on host (Docker-created files). Attempting force removal via Docker...")
                    try:
                        parent = active_path.resolve().parent
                        target = active_path.name
                        cmd = [
                            "docker", "run", "--rm",
                            "-v", f"{parent}:/cleanup_mount",
                            "--entrypoint", "rm",
                            "clickhouse/clickhouse-server:24.3",
                            "-rf", f"/cleanup_mount/{target}"
                        ]
                        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
                        
                        if not active_path.exists():
                            log_success(f"Deleted {active_path} (via Docker).")
                            return
                    except subprocess.CalledProcessError as docker_err:
                        log_error(f"Docker force removal failed: {docker_err.stderr.decode().strip()}")
                
                log_error(f"Failed to delete {active_path}: {e}")
        return

    if reset_all:
        console.print("[bold red]FACTORY RESET PROTOCOL INITIATED[/bold red]")
        steps = [
            "Stop and Remove Docker Containers",
            "Delete User Configuration (~/.config/leakharvester)",
            "Delete Project Configuration (lh-settings.json)",
            "Delete Active Data Directory"
        ]
        for step in steps:
            console.print(f"[red][ ][/red] {step}")
        
        if not Confirm.ask("Proceed with Factory Reset?"):
            return
            
        confirmation = Prompt.ask("Type 'reset all' to confirm")
        if confirmation != "reset all":
            log_info("Reset cancelled.")
            return
            
        log_info("Stopping Docker...")
        subprocess.run(["docker", "compose", "down", "-v"], check=False)
        
        log_info("Removing Configs...")
        if sm.home_config.exists():
            sm.home_config.unlink()
        if sm.local_config.exists():
            sm.local_config.unlink()
            
        active_path = sm.get_active_db_path()
        if active_path and active_path.exists():
             log_info(f"Removing Data at {active_path}...")
             shutil.rmtree(active_path)

        log_success("Factory Reset Complete. System is clean.")
        return

    active_path = sm.get_active_db_path()
    if active_path:
        log_info(f"Active Database: {active_path}")
    else:
        log_warning("No database path configured. Use --path to set one.")
