"""
LeakHarvester CLI entry point.
"""
import typer
from rich.console import Console
from leakharvester.cli.commands.db import db_command
from leakharvester.cli.commands.index import index_command
from leakharvester.cli.commands.search import search_command

app = typer.Typer(
    help="LeakHarvester: High-performance breach data ingestion and search engine.",
    add_completion=False,
)
console = Console()

app.command(name="db")(db_command)
app.command(name="index")(index_command)
app.command(name="search")(search_command)

@app.callback()
def main_callback():
    """
    LeakHarvester CLI.
    """
    pass

if __name__ == "__main__":
    app()