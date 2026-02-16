import typer
from pathlib import Path
from typing import Optional, List, Dict
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


def get_docker_cmd(compose_file: Optional[str] = None) -> Optional[List[str]]:
    """Detects available Docker Compose command and optionally appends file path."""
    cmd = None
    if shutil.which("docker"):
        try:
            subprocess.run(["docker", "compose", "version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
            cmd = ["docker", "compose"]
        except subprocess.CalledProcessError:
            pass
    
    if not cmd and shutil.which("docker-compose"):
        cmd = ["docker-compose"]
        
    if cmd and compose_file:
        cmd.extend(["-f", compose_file])
        
    return cmd

def get_clickhouse_env() -> Dict[str, str]:
    """Prepares environment variables for Docker Compose."""
    sm = SettingsManager()
    active_path = sm.get_active_db_path()
    
    env = os.environ.copy()
    
    runtime_root = sm.get_container_runtime_root()
    env["LOG_PATH"] = str(runtime_root / "clickhouse_logs")
    env["CONFIG_PATH"] = str(runtime_root / "clickhouse_config")

    if active_path:
        env["DB_VOLUME_PATH"] = str(active_path.resolve())
    else:
        env["DB_VOLUME_PATH"] = str(runtime_root / "clickhouse_data")
    return env

def ensure_db_running(force_restart: bool = False, verbose: bool = False):
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
    sm.ensure_docker_compose_exists()
    compose_path = str(sm.get_docker_compose_path())
    
    env = get_clickhouse_env()
    
    # Ensure DB volume is writable by container (UID 101)
    try:
        db_vol = Path(env["DB_VOLUME_PATH"])
        db_vol.mkdir(parents=True, exist_ok=True)
        
        # Only chmod if we own it or are root
        if db_vol.stat().st_uid == os.getuid() or os.getuid() == 0:
             db_vol.chmod(0o777)
        else:
             # It's likely owned by the container user (101) already
             pass
    except Exception as e:
        log_warning(f"Could not check/set permissions on DB path {env['DB_VOLUME_PATH']}: {e}")

    docker_cmd = get_docker_cmd(compose_path)

    if not docker_cmd:
        log_warning("Docker or Docker Compose not found. Cannot auto-start database.")
        log_info(f"Please ensure ClickHouse is running manually at {host}:{port}.")

        if not is_online():
            log_error(f"Connection failed. Is ClickHouse running at {host}:{port}?")
            raise typer.Exit(1)
        return
    
    if not Path(compose_path).exists():
        log_warning(f"No docker-compose.yml found at {compose_path}. Cannot auto-start database.")
        return

    if force_restart:
        log_info("Stopping existing container...")
        subprocess.run(docker_cmd + ["down"], check=False, env=env, stdout=None if verbose else subprocess.DEVNULL, stderr=None if verbose else subprocess.DEVNULL)

    log_info(f"Starting ClickHouse via Docker (Volume: {env['DB_VOLUME_PATH']})...")
    
    up_args = ["up", "-d"]
    if force_restart:
        up_args.append("--force-recreate")
    up_args.append("clickhouse")

    try:
        # If verbose, we let stdout/stderr flow to console. If not, we capture to print on error.
        if verbose:
            subprocess.run(docker_cmd + up_args, check=True, env=env)
        else:
            subprocess.run(docker_cmd + up_args, check=True, env=env, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        if verbose:
            log_error("Docker command failed.")
        else:
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
    for i in range(120):
        if is_online():
            log_success("Database came online!")
            return
        time.sleep(1)
    
    log_error("Database failed to respond after 120 seconds.")
    
    # Debugging: Dump logs from file or container
    log_file = Path(env["LOG_PATH"]) / "clickhouse-server.err.log"
    
    if log_file.exists():
        log_warning(f"Dumping error log from host ({log_file}):")
        try:
            subprocess.run(["tail", "-n", "50", str(log_file)], check=False)
        except Exception as e:
            log_error(f"Failed to read log file: {e}")
    else:
        log_warning("Error log file not found on host. Dumping container logs:")
        try:
            subprocess.run(docker_cmd + ["logs", "--tail", "50", "clickhouse"], check=False, env=env)
        except Exception as log_ex:
            log_error(f"Failed to retrieve logs: {log_ex}")

    raise typer.Exit(1)

def get_ddl_sql(compression_level: int) -> str:
    return f"""
CREATE DATABASE IF NOT EXISTS vault;

CREATE TABLE IF NOT EXISTS vault.breach_records
(
    `source_file` LowCardinality(String) CODEC(ZSTD({compression_level})),
    `breach_date` Date CODEC(Delta(2), ZSTD({compression_level})),
    `import_date` DateTime DEFAULT now() CODEC(Delta(4), ZSTD({compression_level})),
    `email` String CODEC(ZSTD({compression_level})),
    `username` String CODEC(ZSTD({compression_level})),
    `password` String CODEC(ZSTD({compression_level}))
)
ENGINE = MergeTree
ORDER BY (email, source_file)
PARTITION BY source_file
SETTINGS
    index_granularity = 8192,
    max_bytes_to_merge_at_min_space_in_pool = 16777216,
    min_bytes_for_wide_part = 10485760,
    old_parts_lifetime = 30;

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
    compression: int = typer.Option(3, "--compression", "-c", help="Set compression level (1-19) for new tables."),
    status: bool = typer.Option(False, "--status", "-s", help="Check database and Docker status."),
    lsfiles: bool = typer.Option(False, "--lsfiles", help="List all ingested source files."),
    rmfile: str = typer.Option(None, "--rmfile", help="Remove specific files (comma-separated)."),
    allfiles: bool = typer.Option(False, "--allfiles", help="Wipe ALL data (Truncate Table). Instant space reclamation."),
    remove: bool = typer.Option(False, "--remove", "-r", help="Remove the active database data (Stop & Delete)."),
    reset_all: bool = typer.Option(False, "--reset-all", help="FACTORY RESET: Wipes Config, Data, and Docker containers."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose logging.")
):
    """
    Database Lifecycle Management.
    """
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

    if status:
        active_path = sm.get_active_db_path()
        console.print(f"[bold]Active Configured Path:[/bold] {active_path or '[dim]Not configured (Using defaults)[/dim]'}")
        
        ensure_db_running(verbose=verbose)
        try:
            repo = ClickHouseAdapter()
            stats = repo.get_table_stats("vault.breach_records")
            console.print("[green]Database is Online.[/green]")
            console.print(f"Records: {stats['total_rows']:,}")
        except Exception as e:
            console.print(f"[red]Database connection failed:[/red] {e}")
        return

    if lsfiles:
        ensure_db_running(verbose=verbose)
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
        ensure_db_running(verbose=verbose)
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
        ensure_db_running(verbose=verbose)
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
        if not (1 <= compression <= 19):
            log_error("Compression level must be between 1 and 19.")
            raise typer.Exit(1)
            
        ensure_db_running(force_restart=True, verbose=verbose)
        try:
            settings.create_dirs()
            sm.set_compression_level(compression)
            repo = ClickHouseAdapter()
            
            statements = get_ddl_sql(compression).split(";")
            for statement in statements:
                if statement.strip():
                    repo.execute_ddl(statement)
            
            instance_id = str(uuid.uuid4())
            sm.set_instance_id(instance_id)
            log_info(f"Generated Instance ID: {instance_id}")
            
            repo.client.insert("vault.system_info", [[ 'instance_id', instance_id ]], column_names=['key', 'value'])
            
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
            sm.ensure_docker_compose_exists()
            compose_path = str(sm.get_docker_compose_path())
            docker_cmd = get_docker_cmd(compose_path)
            
            if docker_cmd:
                subprocess.run(docker_cmd + ["down"], check=False)
            else:
                log_warning("Could not determine docker command to stop database. Proceeding with file deletion...")
            
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
        console.print("[bold red]FACTORY RESET[/bold red]")
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
        
        try:
            sm.ensure_docker_compose_exists()
            compose_path = str(sm.get_docker_compose_path())
            docker_cmd = get_docker_cmd(compose_path)
        except Exception:
            docker_cmd = get_docker_cmd()

        if docker_cmd:
            env = get_clickhouse_env()
            try:
                log_info("Force stopping containers...")
                subprocess.run(docker_cmd + ["kill"], check=False, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                
                subprocess.run(docker_cmd + ["down", "-v"], check=False, env=env)
            except Exception as e:
                log_warning(f"Failed to stop Docker containers: {e}")
        else:
            log_warning("Docker command not found. Skipping container shutdown.")
        
        log_info("Ensuring no zombie processes remain...")
        try:
            subprocess.run(["pkill", "-9", "-f", "clickhouse"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1)
            
            check = subprocess.run(["pgrep", "-f", "clickhouse"], capture_output=True, text=True)
            if check.returncode == 0:
                pids_raw = check.stdout.strip().split('\n')
                pids_display = ", ".join(pids_raw)
                pids_cmd = " ".join(pids_raw)
                
                log_warning(f"Zombie ClickHouse processes detected (PIDs: {pids_display}). Escalate privileges to kill...")
                
                try:
                    subprocess.run(["sudo", "pkill", "-9", "-f", "clickhouse"], check=True)
                    log_success("Zombie processes killed via sudo.")
                except subprocess.CalledProcessError:
                    subprocess.run(["stty", "sane"], check=False)
                    Console().print(f"[bold red]CRITICAL: Could not kill zombie processes (PIDs: {pids_display}).[/bold red]")
                    Console().print("[bold yellow]Please run this command manually:[/bold yellow]")
                    Console().print(f"[bold]sudo kill -9 {pids_cmd}[/bold]")
        except Exception as e:
            log_warning(f"Zombie cleanup check failed: {e}")

        log_info("Removing Configs...")
        if sm.home_config.exists():
            sm.home_config.unlink()
        if sm.local_config.exists():
            sm.local_config.unlink()
            
        active_path = sm.get_active_db_path()
        if active_path and active_path.exists():
            log_info(f"Removing Data at {active_path}...")
            try:
                shutil.rmtree(active_path)
            except Exception as e:
                if isinstance(e, PermissionError) or (hasattr(e, 'errno') and e.errno == 13):
                    log_warning("Permission denied on host. Attempting force removal via Docker...")
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
                    except subprocess.CalledProcessError as docker_err:
                        log_error(f"Docker force removal failed: {docker_err.stderr.decode().strip()}")
                else:
                    log_error(f"Failed to delete {active_path}: {e}")

        log_success("Factory Reset Complete. System is clean.")
        return

    active_path = sm.get_active_db_path()
    if active_path:
        log_info(f"Active Database: {active_path}")
    else:
        log_warning("No database path configured. Use --path to set one.")
