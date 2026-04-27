import typer
from rich.panel import Panel
from rich import box
from rich.console import Console
from rich.table import Table
from leakharvester.adapters.console import log_error
from leakharvester.adapters.clickhouse import ClickHouseAdapter

def info_command(
    limit: int = typer.Option(50, help="Max source files to list.")
):
    """Displays comprehensive statistics about the breach database."""
    repo = ClickHouseAdapter()
    console = Console()
    
    try:
        stats = repo.get_table_stats("vault.breach_records")
        cols = repo.get_columns("vault.breach_records")
        indices = repo.get_indices("vault.breach_records")
        sources = repo.get_source_file_stats("vault.breach_records", limit)
    except Exception as e:
        log_error(f"Failed to fetch info: {e}")
        return

    overview_table = Table.grid(padding=1, expand=False)
    overview_table.add_column(style="bold cyan", justify="right")
    overview_table.add_column(style="white")
    
    overview_table.add_row("Total Records:", f"{stats['total_rows']:,}")
    overview_table.add_row("Compressed Size:", str(stats['compressed_size']))
    overview_table.add_row("Uncompressed:", str(stats['uncompressed_size']))
    overview_table.add_row("Compression Ratio:", f"{stats['compression_ratio']}x")
    overview_table.add_row("Total Columns:", str(len(cols)))
    overview_table.add_row("Active Indices:", str(len(indices)))

    overview_panel = Panel(
        overview_table,
        title="[bold blue]Database Overview[/bold blue]", 
        border_style="blue",
        box=box.ROUNDED,
        expand=False
    )

    schema_text = ", ".join([f"[green]{c}[/green]" for c in cols])
    schema_panel = Panel(
        schema_text, 
        title="[bold green]Current Schema[/bold green]", 
        border_style="green",
        box=box.ROUNDED,
        expand=False
    )
    
    idx_table = Table(box=box.SIMPLE_HEAD, expand=False)
    idx_table.add_column("Index Name", style="yellow")
    idx_table.add_column("Type")
    idx_table.add_column("Granularity")
    idx_table.add_column("Size", justify="right")
    
    for idx in indices:
        size_str = idx[4] if len(idx) > 4 else "N/A"
        idx_table.add_row(idx[0], idx[1], str(idx[3]), size_str)
        
    idx_panel = Panel(
        idx_table,
        title="[bold yellow]Skipping Indices[/bold yellow]",
        border_style="yellow",
        box=box.ROUNDED,
        expand=False
    )

    src_table = Table(title=f"Top {limit} Source Files", box=box.MINIMAL_DOUBLE_HEAD, expand=False)
    src_table.add_column("Source File", style="bold magenta")
    src_table.add_column("Rows", justify="right")
    src_table.add_column("First Import", style="dim")
    src_table.add_column("Last Import", style="dim")
    
    for src in sources:
        src_table.add_row(
            src[0], 
            f"{src[1]:,}", 
            str(src[2]), 
            str(src[3])
        )

    console.print(overview_panel)
    console.print(schema_panel)
    if indices:
        console.print(idx_panel)
    console.print(src_table)
