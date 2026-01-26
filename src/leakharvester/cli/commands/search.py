import typer
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich import box
from rich.table import Table
import csv
import io
import time
from leakharvester.adapters.console import log_info, log_error, log_warning, log_success
from leakharvester.adapters.clickhouse import ClickHouseAdapter

def search_command(
    query: str = typer.Argument(None, help="Search term (e.g. 'augusto.bachini', 'password123')"),
    limit: int = typer.Option(20, "-l", "--limit", help="Max results to display (0 for unlimited)."),
    column: str = typer.Option(None, "-c", "--column", help="Target specific columns (comma-separated)."),
    
    # Advanced Filtering
    exact: bool = typer.Option(False, "-e", "--exact", help="Exact match (Defaults to case-insensitive unless -C is used)."),
    case: bool = typer.Option(False, "-C", "--case", help="Case sensitive matching."),
    string_mode: bool = typer.Option(False, "-s", "--string", help="Force Full Table Scan (Ignore Indexes)."),
    
    # Presentation
    full: bool = typer.Option(False, "--full", help="Disable smart suppression (Show all columns including empty/dates)."),
    print_columns: str = typer.Option(None, "-p", "--print-column", help="Columns to display in output (comma-separated)."),
    
    # I/O
    quiet: bool = typer.Option(False, "-q", "--quiet", help="Quiet mode (Data only, no banners)."),
    csv_sep: str = typer.Option(None, "--csv", help="Output to CSV with separator (e.g. ',')."),
    output: Path = typer.Option(None, "-o", "--output", help="Save pretty output to file."),
    output_csv: Path = typer.Option(None, "--o-csv", help="Save CSV output to file (respects --csv separator)."),
):
    """
    Searches the breach database with advanced filtering and flexible output.
    
    [bold]Search Modes:[/bold]
    Default: [cyan]ILIKE '%term%'[/cyan] (Fuzzy, Case-Insensitive)
    -e:      [cyan]lower(col) = lower('term')[/cyan] (Exact, Case-Insensitive)
    -C:      [cyan]col LIKE '%term%'[/cyan] (Fuzzy, Case-Sensitive)
    -e -C:   [cyan]col = 'term'[/cyan] (Exact, Case-Sensitive)
    """
    console = Console(quiet=quiet)
    repo = ClickHouseAdapter()
    
    # 1. Column Introspection
    try:
        all_cols = repo.get_columns("vault.breach_records")
    except Exception as e:
        log_error(f"Failed to fetch table schema: {e}")
        return

    # Helper to list columns and exit
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

    # Handle explicit column listing request via empty -p (Typer restriction workaround logic if needed)
    # If no query and no explicit listing, but user asked for search help... Typer handles --help.
    # If user provided -c but invalid, we list columns.
    
    if not query:
        # If no query provided, assume they want to see what's available
        list_cols_and_exit()

    # 2. Target Columns Logic
    search_cols = []
    if column:
        requested = [c.strip() for c in column.split(",")]
        invalid = [c for c in requested if c not in all_cols]
        if invalid:
            log_error(f"Invalid columns: {invalid}")
            list_cols_and_exit()
        search_cols = requested
    else:
        # Default: Search String-like columns, skip dates/metadata if not --full (but search needs to hit text)
        search_cols = [c for c in all_cols if c not in ('breach_date', 'import_date', 'source_file')]
        if not quiet:
            log_info(f"Searching in default columns: {', '.join(search_cols)}")

    # 3. SQL Construction
    conditions = []
    
    # Sanitize query to prevent basic injection if not using parameters (ClickHouse Python driver handles params but we build dynamic SQL)
    # Ideally use parameters, but dynamic column ORs are tricky. We will escape single quotes.
    safe_query = query.replace("'", "\'") 
    
    for col in search_cols:
        if exact and case:
            conditions.append(f"{col} = '{safe_query}'")
        elif exact and not case:
            conditions.append(f"lower({col}) = lower('{safe_query}')")
        elif not exact and case:
            conditions.append(f"{col} LIKE '%{safe_query}%'")
        else:
            # Default
            conditions.append(f"{col} ILIKE '%{safe_query}%'")
            
    where_clause = " OR ".join(conditions)
    
    # Settings
    settings_clause = ""
    if string_mode:
        # Force ignore data skipping indices (Bloom Filters)
        # Fix: ClickHouse 24.3 does not support wildcard '*', we must list indices explicitly.
        try:
            indices = repo.get_indices("vault.breach_records")
            idx_names = [i[0] for i in indices]
            if idx_names:
                ignored_list = ",".join(idx_names)
                settings_clause = f"SETTINGS ignore_data_skipping_indices='{ignored_list}'"
        except Exception as e:
            if not quiet:
                log_warning(f"Failed to fetch indices for ignore list: {e}")

    # Limit Logic (0 = Unlimited)
    limit_clause = f"LIMIT {limit}" if limit > 0 else ""
    
    sql = f"""
        SELECT *
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
        
        # Check for Index Coverage (Warning) if not string mode
        if not string_mode:
            try:
                # We need to re-fetch indices here if we didn't already for string_mode logic, 
                # but effectively get_indices is cheap enough or cached.
                # However, logic structure suggests we might want to deduplicate calls if optimized, 
                # but keeping it robust for now.
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
                
                unindexed = [c for c in search_cols if c not in optimized_cols]
                if unindexed:
                    console.print(f"[yellow]⚠ Warning: Full Table Scan detected on columns: {unindexed}[/yellow]")
                    console.print("[dim]  Performance may be slow. Run 'leakharvester index --auto-optimize' to index them.[/dim]")
            except Exception:
                pass

    start_time = time.time()
    
    try:
        # We use select * so columns are all_cols
        result = repo.client.query(sql)
        elapsed = time.time() - start_time
        rows = result.result_rows
        cols = result.column_names
        
        if not rows:
            if not quiet:
                console.print(f"[yellow]No results found for '{query}'.[/yellow] (Time: {elapsed:.2f}s)")
            return
            
        # 4. Smart Suppression & Column Filtering
        
        # Determine cols to display
        display_cols = cols
        if print_columns:
            requested_disp = [c.strip() for c in print_columns.split(",")]
            # Validate
            invalid_disp = [c for c in requested_disp if c not in cols]
            if invalid_disp:
                log_error(f"Invalid columns for output: {invalid_disp}")
                list_cols_and_exit()
            display_cols = requested_disp
        
        # Logic: If --full is OFF, filter out:
        # 1. 'breach_date' (Explicit suppression)
        # 2. Columns that are entirely None/Empty in this result set
        
        final_cols = []
        final_indices = []
        
        for i, col_name in enumerate(cols):
            if col_name not in display_cols:
                continue
                
            if not full:
                if col_name == 'breach_date':
                    continue
                # Check for emptiness
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

        # Transform Rows
        final_rows = []
        for row in rows:
            new_row = [row[i] for i in final_indices]
            final_rows.append(new_row)

        # 5. Output Handling
        
        # CSV Generation
        csv_buffer = io.StringIO()
        sep = csv_sep if csv_sep else ","
        writer = csv.writer(csv_buffer, delimiter=sep)
        writer.writerow(final_cols)
        writer.writerows(final_rows)
        csv_output = csv_buffer.getvalue()
        
        # Case A: CSV to Stdout
        if csv_sep and not output_csv:
            print(csv_output, end="")
            return

        # Case B: CSV to File
        if output_csv:
            output_csv.write_text(csv_output, encoding="utf-8")
            if not quiet:
                log_success(f"CSV saved to {output_csv}")

        # Case C: Pretty Table to Stdout (Default) OR File
        if not csv_sep:
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
                    log_success(f"Results saved to {output}")
            elif not output_csv: # Only print if not saving CSV exclusive
                console.print(table)
        
    except Exception as e:
        log_error(f"Search failed: {e}")
