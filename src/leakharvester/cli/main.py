import sys
import typer
from leakharvester.cli.commands.db import db_command
from leakharvester.cli.commands.index import index_command
from leakharvester.cli.commands.search import search_command
from leakharvester.cli.commands.ingest import ingest_command
from leakharvester.cli.commands.repair import repair_command
from leakharvester.cli.commands.info import info_command
from leakharvester.cli.commands.secure_io import export_command, import_command
from leakharvester.cli.ui import configure_typer_ui

configure_typer_ui()

app = typer.Typer(
    help="LeakHarvester: High-performance CLI for ingesting, indexing, and searching massive breach datasets.",
    add_completion=False,
    rich_markup_mode="rich",
)

app.command(name="db")(db_command)
app.command(name="index")(index_command)
app.command(name="search")(search_command)
app.command(name="ingest")(ingest_command)
app.command(name="repair")(repair_command)
app.command(name="info")(info_command)
app.command(name="export")(export_command)
app.command(name="import")(import_command)

def lhs_entry():
    if len(sys.argv) > 1 and sys.argv[1] != "search":
        sys.argv.insert(1, "search")
    elif len(sys.argv) == 1:
        sys.argv.append("search")
    app()

if __name__ == "__main__":
    app()
