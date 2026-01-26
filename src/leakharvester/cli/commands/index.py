import typer
from rich.table import Table
from rich.prompt import Prompt, Confirm
from rich.console import Console
from leakharvester.adapters.console import log_info, log_error
from leakharvester.adapters.clickhouse import ClickHouseAdapter
from leakharvester.services.index_optimizer import IndexManager, HeuristicAnalyzer

def index_command(
    # Targeting
    column: str = typer.Option(None, "-c", "--column", help="Target specific columns (comma-separated)."),
    remove: bool = typer.Option(False, "-r", "--remove", help="Drop existing indexes on targeted columns."),
    list_indexes: bool = typer.Option(False, "-l", "--list", help="List active indexes."),
    
    # Automation
    auto_optimize: bool = typer.Option(False, "-a", "--auto-optimize", help="Run [bold green]Heuristic Analyzer[/bold green] to automatically recommend indexes."),
    auto_random: bool = typer.Option(False, "--auto-random", help="Use random sampling for analyzer (Slower but unbiased)."),

    # Manual Overrides
    tokenbf: bool = typer.Option(False, "-t", "--tokenbf", help="Apply [cyan]Token Bloom Filter[/cyan]."),
    tokenbf_size: int = typer.Option(32768, "--tokenbf-size", help="Bloom filter size in bytes."),
    tokenbf_hash: int = typer.Option(3, "--tokenbf-hash", help="Number of hash functions."),
    tokenbf_seed: int = typer.Option(0, "--tokenbf-seed", help="Random seed."),
    
    ngram: bool = typer.Option(False, "-n", "--ngram", help="Apply [cyan]N-Gram Bloom Filter[/cyan]."),
    ngram_n: int = typer.Option(4, "--ngram-n", help="Gram size (e.g. 4 for trigrams)."),
    ngram_size: int = typer.Option(32768, "--ngram-size", help="Bloom filter size in bytes."),
    
    inverted: bool = typer.Option(False, "-i", "--inverted", help="Apply [cyan]Inverted Index[/cyan] (ClickHouse native)."),
):
    """
    Intelligent Database Indexing Orchestrator.
    Manages performance indexes using Heuristic Analysis or Manual Configuration.

    [bold]INDEX TYPES REFERENCE:[/bold]

    [cyan]1. Inverted Index (Log/Inverted)[/cyan]
    [dim]Technical:[/dim] Maps terms to row IDs (Segment postings). ClickHouse `inverted(0)`.
    [green]Pros:[/green] Excellent for full-text search, high cardinality exact matches, and `hasToken` queries.
    [red]Cons:[/red] High storage overhead for unique text.

    [cyan]2. Token Bloom Filter (TokenBF)[/cyan]
    [dim]Technical:[/dim] Splits string into tokens (by non-alphanumeric), hashes them into a Bloom Filter.
    [green]Pros:[/green] Very small storage. Good for checking if a token exists in a block.
    [red]Cons:[/red] False positives possible. Cannot find substrings *inside* tokens (e.g. 'pass' in 'password').

    [cyan]3. N-Gram Bloom Filter (NgramBF)[/cyan]
    [dim]Technical:[/dim] Splits string into N-sized grams (sliding window).
    [green]Pros:[/green] Finds arbitrary substrings (e.g., '123' in 'password123').
    [red]Cons:[/red] Larger storage than TokenBF. Computationally expensive to build.

    [bold]EXAMPLES:[/bold]
      [yellow]leakharvester index --auto-optimize[/yellow] (Recommended)
      [yellow]leakharvester index -c email -i[/yellow] (Manual Inverted Index on Email)
      [yellow]leakharvester index -c password -n --ngram-n 3[/yellow] (Trigram index on Password)
    """
    repo = ClickHouseAdapter()
    manager = IndexManager(repo)
    console = Console()
    
    # 1. LIST MODE
    if list_indexes:
        idxs = manager.list_indexes()
        table = Table(title="Active Indexes")
        table.add_column("Name", style="cyan")
        table.add_column("Column", style="magenta")
        table.add_column("Type", style="green")
        table.add_column("Granularity")
        table.add_column("Size", justify="right", style="bold yellow")
        
        for i in idxs:
            table.add_row(
                i['name'], 
                i['column'], 
                i['type'], 
                str(i['granularity']), 
                i.get('size', 'N/A')
            )
        console.print(table)
        return

    # 2. TARGET VALIDATION
    target_cols = []
    if column:
        # Validate columns
        all_cols = repo.get_columns("vault.breach_records")
        requested = [c.strip() for c in column.split(",")]
        invalid = [c for c in requested if c not in all_cols]
        if invalid:
            log_error(f"Invalid columns: {invalid}")
            console.print(f"Valid columns: {', '.join(all_cols)}")
            raise typer.Exit(1)
        target_cols = requested
    elif auto_optimize:
        # Auto targets all String columns if not specified
        target_cols = repo.get_columns("vault.breach_records")
    else:
        # If no column and no list/auto, show help or error
        if not remove: 
             log_error("Please specify columns (-c) or enable automation (-a) or list (-l).")
             raise typer.Exit(1)

    # 3. REMOVE MODE
    if remove:
        if not column:
            log_error("Please specify columns to remove indexes from.")
            raise typer.Exit(1)
        
        if Confirm.ask(f"Drop indexes for {target_cols}?"):
            for col in target_cols:
                manager.drop_index(col)
        return

    # 4. MANUAL CONFIGURATION
    if tokenbf or ngram or inverted:
        if not column:
            log_error("Manual index configuration requires target columns (-c).")
            raise typer.Exit(1)
            
        ddl = ""
        if inverted:
            ddl = "TYPE inverted(0) GRANULARITY 1"
        elif tokenbf:
            ddl = f"TYPE tokenbf_v1({tokenbf_size}, {tokenbf_hash}, {tokenbf_seed}) GRANULARITY 1"
        elif ngram:
            ddl = f"TYPE ngrambf_v1({ngram_n}, {ngram_size}, 2, 0) GRANULARITY 1"
            
        for col in target_cols:
            manager.apply_index(col, ddl)
        return

    # 5. AUTO OPTIMIZATION (Interactive)
    if auto_optimize:
        analyzer = HeuristicAnalyzer(repo)
        
        for col in target_cols:
            sample_size = 10000
            while True:
                console.print(f"\n[bold]Analyzing column: {col}[/bold]")
                rec = analyzer.analyze_column("vault.breach_records", col, sample_size, auto_random)
                
                # Render Report
                rtable = Table(show_header=False, box=None)
                rtable.add_row("Recommendation:", f"[{'green' if rec.confidence > 0.8 else 'yellow'}]{rec.type}[/]")
                rtable.add_row("Confidence:", f"{rec.confidence:.0%}")
                rtable.add_row("Reason:", rec.reason)
                console.print(rtable)
                
                if rec.type == "NONE" and rec.confidence > 0.9:
                    log_info(f"Skipping {col} (No index recommended).")
                    break

                # Prompt
                menu_legend = """
  [bold cyan][A][/bold cyan]ccept Recommendation (Default)
  [bold cyan][S][/bold cyan]elect Manual Index
  [bold cyan][D][/bold cyan]eep Analyze (Increase Sample Size)
  S[bold cyan][K][/bold cyan]ip Column
  [bold cyan][Q][/bold cyan]uit
"""
                console.print(menu_legend)
                
                choice = Prompt.ask(
                    "Action", 
                    choices=["a", "s", "d", "k", "q"],
                    default="a"
                ).lower()
                
                if choice == "q":
                    raise typer.Exit()
                elif choice == "k":
                    log_info("Skipped.")
                    break
                elif choice == "a":
                    if rec.type != "NONE":
                        manager.apply_index(col, rec.ddl_params)
                    break
                elif choice == "d":
                    sample_size *= 2
                    log_info(f"Deep analyzing with {sample_size} rows...")
                    continue # Re-loop
                elif choice == "s":
                    # Simple manual override selection
                    sel = Prompt.ask("Select Type", choices=["inverted", "tokenbf", "ngram", "none"])
                    if sel == "none": break
                    
                    # Defaults for manual override via this menu
                    if sel == "inverted": ddl = "TYPE inverted(0) GRANULARITY 1"
                    elif sel == "tokenbf": ddl = "TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1"
                    elif sel == "ngram": ddl = "TYPE ngrambf_v1(4, 32768, 2, 0) GRANULARITY 1"
                    
                    manager.apply_index(col, ddl)
                    break
