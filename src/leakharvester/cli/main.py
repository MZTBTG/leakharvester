import typer
from leakharvester.cli.commands.db import db_command
from leakharvester.cli.commands.index import index_command
from leakharvester.cli.commands.search import search_command
from leakharvester.cli.commands.ingest import ingest_command
from leakharvester.cli.commands.repair import repair_command
from leakharvester.cli.commands.info import info_command
from leakharvester.cli.commands.secure_io import export_command, import_command

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

if __name__ == "__main__":
    app()