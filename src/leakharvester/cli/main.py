"""
LeakHarvester CLI entry point.
"""
import typer
import rich_click
from rich.console import Console
from leakharvester.cli.commands.db import db_command
from leakharvester.cli.commands.index import index_command
from leakharvester.cli.commands.search import search_command
from leakharvester.cli.commands.ingest import ingest_command
from leakharvester.cli.commands.repair import repair_command
from leakharvester.cli.commands.info import info_command
from leakharvester.cli.commands.secure_io import export_command, import_command

# --- Rich-Click Configuration ---
# Force adaptive width and disable full-width expansion
rich_click.rich_click.USE_RICH_MARKUP = True
rich_click.rich_click.USE_MARKDOWN = True
rich_click.rich_click.SHOW_ARGUMENTS = True
rich_click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
rich_click.rich_click.STYLE_ERRORS_SUGGESTION = "magenta italic"
rich_click.rich_click.ERRORS_SUGGESTION = "Try running the '--help' flag for more information."
rich_click.rich_click.ERRORS_EPILOGUE = "To find out more, visit https://github.com/mztb/leakharvester"
rich_click.rich_click.MAX_WIDTH = 100  # Set a reasonable max width to prevent spanning ultra-wide terminals
rich_click.rich_click.STYLE_HELPTEXT_FIRST_LINE = "bold cyan"

# Panel Configuration for Adaptive Look
# Note: rich-click doesn't expose a direct 'expand=False' for the main help panel easily 
# without monkeypatching, but limiting MAX_WIDTH helps.
# For Errors, we want them to be distinct.

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