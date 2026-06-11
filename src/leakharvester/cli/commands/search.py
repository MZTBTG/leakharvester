import typer
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich import box
from rich.table import Table
import time
import sys
from leakharvester.adapters.console import log_info, log_error, log_warning, log_success
from leakharvester.adapters.clickhouse import ClickHouseAdapter

def search_command(
    query: str = typer.Argument(None, help="Search term (e.g. 'vishmaria@example.com', 'password123')"),
    limit: int = typer.Option(20, "-l", "--limit", help="Max results to display (0 for unlimited)."),
    column: str = typer.Option(None, "-c", "--column", help="Target specific columns (comma-separated)."),
    exact: bool = typer.Option(False, "-e", "--exact", help="Exact full-string match (case-insensitive unless -C is used)."),
    case: bool = typer.Option(False, "-C", "--case", help="Case sensitive matching."),
    string_mode: bool = typer.Option(False, "-s", "--string", help="Force Full Table Scan (Ignore Indexes)."),
    full: bool = typer.Option(False, "--full", help="Show all columns including empty."),
    print_columns: str = typer.Option(None, "-p", "--print-column", help="Columns to display in output (comma-separated)."),
    quiet: bool = typer.Option(False, "-q", "--quiet", help="Quiet mode (Data only, no banners)."),
    output: Path = typer.Option(None, "-o", "--output", help="Save output to file (CSV by default, or Table if --pretty)."),
    pretty: bool = typer.Option(False, "--pretty", help="Show results in a pretty table (slower, loads all to memory)."),
):
    """
    Searches the breach database with advanced filters.
    
    [bold]Search Modes:[/bold]
    Default: [cyan]ILIKE '%term%'[/cyan] (Fuzzy, Case-Insensitive)
    -e:      [cyan]lower(col) = lower('term')[/cyan] (Exact, Case-Insensitive)
    -C:      [cyan]col LIKE '%term%'[/cyan] (Fuzzy, Case-Sensitive)
    -e -C:   [cyan]col = 'term'[/cyan] (Exact, Case-Sensitive)
    """
    console = Console(quiet=quiet)
    repo = ClickHouseAdapter()
    
    try:
        all_cols = repo.get_columns("vault.breach_records")
    except Exception as e:
        log_error(f"Failed to fetch table schema: {e}")
        return

    def list_cols_and_exit():
        schema_text = ", ".join([f"[green]{c}[/green]" for c in all_cols])
        schema_panel = Panel(
            schema_text, 
            title="[bold green]Available Columns[/bold green]", 
            border_style="green",
            box=box.ROUNDED
        )
        console.print(schema_panel)
        raise typer.Exit()

    
    if not query:
        list_cols_and_exit()
        
    search_cols = []
    if column:
        requested = [c.strip() for c in column.split(",")]
        invalid = [c for c in requested if c not in all_cols]
        if invalid:
            log_error(f"Invalid columns: {invalid}")
            list_cols_and_exit()
        search_cols = requested
    else:
        search_cols = [c for c in all_cols if c not in ('breach_date', 'import_date', 'source_file')]
        if not quiet:
            log_info(f"Searching in columns: {', '.join(search_cols)}")
            
    selected_columns = all_cols
    if print_columns:
        requested_disp = [c.strip() for c in print_columns.split(",")]
        invalid_disp = [c for c in requested_disp if c not in all_cols]
        if invalid_disp:
            log_error(f"Invalid columns for output: {invalid_disp}")
            list_cols_and_exit()
        selected_columns = requested_disp
    else:
        if not pretty and not full:
             selected_columns = [c for c in all_cols if c not in ('breach_date', 'import_date', 'source_file')]

    conditions = []
    safe_query = query.replace("'", "\'") 
    
    for col in search_cols:
        if exact and case:
            conditions.append(f"{col} = '{safe_query}'")
        elif exact and not case:
            conditions.append(f"lower({col}) = lower('{safe_query}')")
        elif not exact and case:
            conditions.append(f"{col} LIKE '%{safe_query}%'")
        else:
            conditions.append(f"{col} ILIKE '%{safe_query}%'")
            
    where_clause = " OR ".join(conditions)
    
    settings_clause = ""
    if string_mode:
        try:
            indices = repo.get_indices("vault.breach_records")
            idx_names = [i[0] for i in indices]
            if idx_names:
                ignored_list = ",".join(idx_names)
                settings_clause = f"SETTINGS ignore_data_skipping_indices='{ignored_list}'"
        except Exception as e:
            if not quiet:
                log_warning(f"Failed to fetch indices for ignore list: {e}")

    limit_clause = f"LIMIT {limit}" if limit > 0 else ""
    
    select_clause = ", ".join(selected_columns)
    
    sql = f"""
        SELECT {select_clause}
        FROM vault.breach_records
        WHERE {where_clause}
        {limit_clause}
        {settings_clause}
    """
    
    if not quiet:
        mode_str = "Exact" if exact else "Fuzzy"
        case_str = "Sensitive" if case else "Insensitive"
        idx_str = " (Full Scan)" if string_mode else ""
        log_info(f"Executing {mode_str}/{case_str} Search: [bold]{query}[/bold] on {len(search_cols)} columns{idx_str}")
        
        if not string_mode:
            try:
                indices = repo.get_indices("vault.breach_records")
                optimized_cols = set()
                for idx in indices:
                    name, type_def = idx[0], idx[1].lower()
                    target_col = None
                    expr = idx[2] if len(idx) > 2 else ""
                    
                    if expr in all_cols:
                        target_col = expr
                    elif name.startswith("idx_"):
                        parts = name.split("_")
                        for c in all_cols:
                            if c in parts:
                                target_col = c
                                break
                    
                    if target_col and any(t in type_def for t in ("inverted", "tokenbf", "ngrambf")):
                        optimized_cols.add(target_col)
                
                unindexed = [c for c in search_cols if c not in optimized_cols and c != 'email']
                if unindexed:
                    console.print(f"[yellow]⚠ Warning: Full Table Scan detected on columns: {unindexed}[/yellow]")
                    console.print("[dim]  Performance may be slow. Run 'leakharvester index --auto-optimize' to index them.[/dim]")
            except Exception:
                pass

    start_time = time.time()
    
    try:
        if not pretty:
            if not quiet:
                 log_info("Streaming raw results...")

            out_file = None
            should_close = False
            
            if output:
                out_file = open(output, 'wb')
                should_close = True
            else:
                out_file = sys.stdout.buffer

            try:
                raw_chunk_stream = repo.stream_raw_query(sql, fmt='CSVWithNames')
                
                byte_count = 0
                for chunk in raw_chunk_stream:
                    out_file.write(chunk)
                    byte_count += len(chunk)
                
                if output and not quiet:
                    elapsed = time.time() - start_time
                    size_mb = byte_count / (1024 * 1024)
                    speed = size_mb / elapsed if elapsed > 0 else 0
                    log_success(f"CSV saved to {output} ({size_mb:.2f} MB, {elapsed:.2f}s, {speed:.2f} MB/s)")
            
            finally:
                if should_close and out_file:
                    out_file.close()
            return

        row_stream = repo.stream_query(sql)
        
        rows = list(row_stream)
        elapsed = time.time() - start_time
        
        if not rows:
            if not quiet:
                console.print(f"[yellow]No results found for '{query}'.[/yellow] (Time: {elapsed:.2f}s)")
            return

        final_cols = []
        final_indices = []
        
        for i, col_name in enumerate(selected_columns):
            if not full:
                if col_name == 'breach_date':
                    continue
                
                is_empty = True
                for row in rows:
                    val = row[i]
                    if val is not None and str(val).strip() != "":
                        is_empty = False
                        break
                if is_empty:
                    continue
            
            final_cols.append(col_name)
            final_indices.append(i)
            
        if not final_cols:
            if not quiet:
                console.print("[yellow]Results found but all columns suppressed (Try --full).[/yellow]")
            return

        final_rows = []
        for row in rows:
            new_row = [row[i] for i in final_indices]
            final_rows.append(new_row)

        table = Table(title=f"Search Results ({len(rows)}) - {elapsed:.2f}s" if not quiet else None)
        for col in final_cols:
            table.add_column(col, style="cyan")
            
        for row in final_rows:
            safe_row = [str(r) if r is not None else "" for r in row]
            table.add_row(*safe_row)
            
        if output:
            with open(output, "w", encoding="utf-8") as f:
                console_file = Console(file=f)
                console_file.print(table)
            if not quiet:
                log_success(f"Table saved to {output}")
        else:
            console.print(table)
        
    except Exception as e:
        log_error(f"Search failed: {e}")

