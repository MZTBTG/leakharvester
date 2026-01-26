import typer
from pathlib import Path
from rich.prompt import Prompt, Confirm
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

# Helper function (previously in main.py)
def ensure_db_running():
    """Checks if ClickHouse is running; attempts to start it via Docker if not."""
    host = settings.CLICKHOUSE_HOST or "localhost"
    port = settings.CLICKHOUSE_PORT or 8123
    ping_url = f"http://{host}:{port}/ping"

    def is_online():
        try:
            with urllib.request.urlopen(ping_url, timeout=1) as response:
                return response.read().strip() == b"Ok."
        except Exception:
            return False

    if is_online():
        return

    log_warning("Database unreachable. Checking for Docker environment...")

    # Load configured DB path
    sm = SettingsManager()
    active_path = sm.get_active_db_path()
    
    # Prepare environment variables for Docker
    env = os.environ.copy()
    if active_path:
        env["DB_VOLUME_PATH"] = str(active_path.resolve())
    else:
        # Default behavior if no path configured
        env["DB_VOLUME_PATH"] = "./data/clickhouse_data"

    # Detect Docker command (prefer 'docker compose', fallback to 'docker-compose')
    docker_cmd = None
    if shutil.which("docker"):
        # Check if 'docker compose' subcommand works
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
    
    # Check for docker-compose.yml or compose.yaml
    if not (Path("docker-compose.yml").exists() or Path("compose.yaml").exists()):
        log_warning("No docker-compose.yml found. Cannot auto-start database.")
        return

    log_info(f"Starting ClickHouse via Docker (Volume: {env['DB_VOLUME_PATH']})...")
    try:
        # Check if container is just stopped or needs to be created
        subprocess.run(docker_cmd + ["up", "-d", "clickhouse"], check=True, env=env, capture_output=True, text=True)
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

    # Wait for health
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
"""

def db_command(
    path: Path = typer.Option(None, "--path", "-p", help="Set or query the active database path."),
    init: bool = typer.Option(False, "--init", help="Initialize the database at the configured path."),
    status: bool = typer.Option(False, "--status", "-s", help="Check database and Docker status."),
    remove: bool = typer.Option(False, "--remove", "-r", help="Remove the active database data (Stop & Delete)."),
    reset_all: bool = typer.Option(False, "--reset-all", help="FACTORY RESET: Wipes Config, Data, and Docker containers.")
):
    """
    Database Lifecycle Management.
    """
    sm = SettingsManager()
    console = Console()

    # 1. PATH MANAGEMENT
    if path:
        if path.exists():
            if not path.is_dir():
                log_error(f"Path {path} is not a directory.")
                raise typer.Exit(1)
            
            # Check for emptiness or validity
            if any(path.iterdir()):
                # Simple check for our expected format (clickhouse structure)
                # Just a warning/info, don't block
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

    # 2. STATUS
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

    # 3. INITIALIZATION
    if init:
        ensure_db_running()
        try:
            settings.create_dirs()
            repo = ClickHouseAdapter()
            
            # Execute DDL statements sequentially
            statements = DDL_SQL.split(";")
            for statement in statements:
                if statement.strip():
                    repo.execute_ddl(statement)
            
            log_success(f"Database initialized at {sm.get_active_db_path() or 'default location'}.")
        except Exception as e:
            log_error(f"Failed to initialize DB: {e}")
            raise typer.Exit(code=1)
        return

    # 4. REMOVE
    if remove:
        active_path = sm.get_active_db_path()
        if not active_path:
            log_error("No active DB path configured. Cannot safely remove default data without explicit path.")
            return

        if Confirm.ask(f"[bold red]DANGER:[/bold red] Stop DB and DELETE all data at {active_path}?"):
            # Stop Docker
            subprocess.run(["docker", "compose", "down"], check=False)
            # Delete files
            try:
                shutil.rmtree(active_path)
                log_success(f"Deleted {active_path}")
            except Exception as e:
                # Check for Permission Error (Errno 13)
                if isinstance(e, PermissionError) or (hasattr(e, 'errno') and e.errno == 13):
                    log_warning("Permission denied on host (Docker-created files). Attempting force removal via Docker...")
                    try:
                        parent = active_path.resolve().parent
                        target = active_path.name
                        # Use the existing image to remove files as root
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

    # 5. FACTORY RESET
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
            
        # Execute
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

    # Default: Show current config
    active_path = sm.get_active_db_path()
    if active_path:
        log_info(f"Active Database: {active_path}")
    else:
        log_warning("No database path configured. Use --path to set one.")
