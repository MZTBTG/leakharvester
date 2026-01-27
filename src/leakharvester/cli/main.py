"""
LeakHarvester CLI entry point.
"""
import typer
import click
from rich.console import Console
from leakharvester.cli.commands.db import db_command
from leakharvester.cli.commands.index import index_command
from leakharvester.cli.commands.search import search_command
from leakharvester.cli.commands.ingest import ingest_command
from leakharvester.cli.commands.repair import repair_command
from leakharvester.cli.commands.info import info_command
from leakharvester.cli.commands.secure_io import export_command, import_command
from leakharvester.adapters.console import log_error

app = typer.Typer(
    help="LeakHarvester: High-performance breach data ingestion and search engine.",
    add_completion=False,
    rich_markup_mode="rich",
)
console = Console()

app.command(name="db")(db_command)
app.command(name="index")(index_command)
app.command(name="search")(search_command)
app.command(name="ingest")(ingest_command)
app.command(name="repair")(repair_command)
app.command(name="info")(info_command)
app.command(name="export")(export_command)
app.command(name="import")(import_command)

@app.callback()
def main_callback():
    """
    LeakHarvester CLI.
    """
    pass

if __name__ == "__main__":
    try:
        app(standalone_mode=False)
    except click.exceptions.Exit:
        pass
    except click.exceptions.UsageError as e:
        log_error(f"{e.message}\n[italic]Try '{e.ctx.command_path} --help' for help.[/italic]")
        exit(1)
    except click.exceptions.Abort:
        log_error("Aborted.")
        exit(1)
    except click.exceptions.ClickException as e:
        log_error(f"{e.message}")
        exit(1)
    except Exception as e:
        log_error(f"Unexpected Error: {e}")
        exit(1)