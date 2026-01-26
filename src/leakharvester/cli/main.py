"""
LeakHarvester CLI entry point.
"""
import typer
from rich.console import Console

app = typer.Typer(
    help="LeakHarvester: High-performance breach data ingestion and search engine.",
    add_completion=False,
)
console = Console()

@app.callback()
def main_callback():
    """
    LeakHarvester CLI.
    """
    pass

if __name__ == "__main__":
    app()
