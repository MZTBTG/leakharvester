"""
LeakHarvester CLI entry point.
"""
import typer
from rich.console import Console
from leakharvester.cli.commands.db import db_command
from leakharvester.cli.commands.index import index_command
from leakharvester.cli.commands.search import search_command
from leakharvester.cli.commands.ingest import ingest_command
from leakharvester.cli.commands.repair import repair_command
from leakharvester.cli.commands.info import info_command
from leakharvester.cli.commands.secure_io import export_command, import_command

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
    app()