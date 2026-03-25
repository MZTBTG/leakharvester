import sys
import os
import subprocess
import time
import uuid
import traceback
from rich.console import Console
from rich.table import Table

# 1. Fix Import Path
sys.path.append(os.path.abspath("src"))

from leakharvester.adapters.clickhouse import ClickHouseAdapter

console = Console()
repo = ClickHouseAdapter()

BENCH_TABLE = "vault.bench_breach_records"
PROD_TABLE = "vault.breach_records"

# Setup Benchmark Table
print(f"Setting up benchmark table: {BENCH_TABLE}...")
try:
    repo.client.command(f"DROP TABLE IF EXISTS {BENCH_TABLE}")
    # Clone structure
    repo.client.command(f"CREATE TABLE {BENCH_TABLE} AS {PROD_TABLE}")
    # Populate with sample data (1M rows)
    print("Populating with sample data (1M rows)...")
    repo.client.command(f"INSERT INTO {BENCH_TABLE} SELECT * FROM {PROD_TABLE} LIMIT 1000000")
    print("Benchmark table ready.")
except Exception as e:
    print(f"[CRITICAL] Failed to setup benchmark table: {e}")
    sys.exit(1)

configs = [
    ("Eco Ngram", ["--ngram"]),
    ("Turbo Ngram", ["--ngram", "--ngram-size", "131072"]),
    ("Eco Token", ["--tokenbf"]),
    ("Turbo Token", ["--tokenbf", "--tokenbf-size", "131072"]),
    ("Inverted", ["--inverted"])
]

results = []

def get_stats():
    bench_table_name = BENCH_TABLE.split(".")[1]
    # Get Part Count (Chunks)
    parts = repo.client.query(f"SELECT count() FROM system.parts WHERE table='{bench_table_name}' AND active=1").result_rows[0][0]
    
    # Get Index Size (Bytes on disk)
    try:
        size_bytes = repo.client.query(f"SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE table='{bench_table_name}' AND active=1").result_rows[0][0]
        size_human = repo.client.query(f"SELECT formatReadableSize({size_bytes})").result_rows[0][0]
    except:
        size_human = "N/A"
        size_bytes = 0
    return parts, size_human, size_bytes

def get_query_metrics(query_id):
    """Fetches read_rows and read_bytes from system.query_log for a specific query_id."""
    # We need to flush the query log first to ensure data is visible
    repo.client.command("SYSTEM FLUSH LOGS")
    time.sleep(0.5)
    try:
        res = repo.client.query(f"""
            SELECT read_rows, read_bytes, memory_usage 
            FROM system.query_log 
            WHERE query_id = '{query_id}' 
            AND type = 'QueryFinish'
            ORDER BY event_time DESC 
            LIMIT 1
        """).result_rows
        if res:
            return res[0] # (read_rows, read_bytes, memory)
    except Exception as e:
        print(f"Error fetching metrics: {e}")
    return (0, 0, 0)

def run_query(name, query_sql, ignore_index=False, use_inverted=False):
    query_id = f"bench_{uuid.uuid4().hex}"
    # High precision timer
    start = time.perf_counter()
    
    # Force max resources
    settings = {
        'max_threads': 20, 
        'max_memory_usage': 40000000000, 
        'query_id': query_id,
        'use_query_cache': 0 # Disable cache for accurate bench
    }
    
    if ignore_index:
        # Standard setting to ignore skipping indexes (verified name)
        settings['ignore_data_skipping_indices'] = 1 
        settings['use_skip_indexes'] = 0 # Double down to be safe 
        
    if use_inverted:
        settings['allow_experimental_inverted_index'] = 1

    try:
        repo.client.query(query_sql, settings=settings)
        elapsed = time.perf_counter() - start
        
        # Get detailed metrics
        read_rows, read_bytes, ram = get_query_metrics(query_id)
        
        return {
            "time": elapsed,
            "read_rows": read_rows,
            "read_bytes": read_bytes
        }
    except Exception as e:
        # ABORT ON ERROR as requested
        print(f"\n[CRITICAL] Query '{name}' Failed!")
        print(f"SQL: {query_sql}")
        print(f"Settings: {settings}")
        print("-" * 60)
        traceback.print_exc()
        raise e # Re-raise to stop execution

# --- Baseline Test (Run on current state before switching) ---
console.rule("Running Baseline (No Index / Ignore Index)")

baseline_partial = run_query(
    "Baseline Partial ILIKE", 
    f"SELECT count() FROM {BENCH_TABLE} WHERE email ILIKE '%augusto.bachini%'", 
    ignore_index=True
)

baseline_like = run_query(
    "Baseline Partial LIKE", 
    f"SELECT count() FROM {BENCH_TABLE} WHERE email LIKE '%augusto.bachini%'", 
    ignore_index=True
)

baseline_token = run_query(
    "Baseline Token", 
    f"SELECT count() FROM {BENCH_TABLE} WHERE hasToken(email, 'bachini')", 
    ignore_index=True,
    use_inverted=True # Allowed but ignored since we set ignore_secondary_indices
)

baseline_full = run_query(
    "Baseline Full", 
    f"SELECT count() FROM {BENCH_TABLE} WHERE email = 'henrique.augusto.bachini@hotmail.com'", 
    ignore_index=True
)

print(f"Baseline ILIKE: {baseline_partial['time']:.4f}s (Scanned {baseline_partial['read_rows']:,} rows)")
print(f"Baseline LIKE:  {baseline_like['time']:.4f}s")
print(f"Baseline Token: {baseline_token['time']:.4f}s")
print(f"Baseline Full:  {baseline_full['time']:.4f}s")

# Main Loop
for name, args in configs:
    console.rule(f"Testing Configuration: {name}")
    
    # 1. Switch Mode (Build Index)
    start_build = time.perf_counter()
    
    print("Removing previous indexes...")
    subprocess.run(["uv", "run", "python", "-m", "leakharvester.cli.main", "index", "-c", "email", "--remove", "--yes", "--table", BENCH_TABLE], check=True)
    
    cmd = ["uv", "run", "python", "-m", "leakharvester.cli.main", "index", "-c", "email", "--table", BENCH_TABLE] + args
        
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    
    # 2. Optimize (Reduce Chunks) - Crucial for "increasing size of chunks"
    print("Forcefully merging data parts (OPTIMIZE FINAL)...")
    try:
        repo.client.command(f"OPTIMIZE TABLE {BENCH_TABLE} FINAL", settings={'receive_timeout': 3600})
    except Exception as e:
        print("[CRITICAL] OPTIMIZE TABLE Failed!")
        traceback.print_exc()
        raise e
        
    build_time = time.perf_counter() - start_build
    
    # 3. Stats
    parts, idx_size_human, idx_size_bytes = get_stats()
    
    # 4. Search Tests
    is_inverted = ("Inverted" in name)
    
    # A. Partial (Substring) - "augusto.bachini"
    partial_res = run_query(
        "Partial ILIKE", 
        f"SELECT count() FROM {BENCH_TABLE} WHERE email ILIKE '%augusto.bachini%'",
        use_inverted=is_inverted
    )
    
    # A.2 Partial (Substring) - Case Sensitive LIKE
    partial_like_res = run_query(
        "Partial LIKE", 
        f"SELECT count() FROM {BENCH_TABLE} WHERE email LIKE '%augusto.bachini%'",
        use_inverted=is_inverted
    )

    # A.3 Token Search - "bachini"
    token_res = run_query(
        "Token Search", 
        f"SELECT count() FROM {BENCH_TABLE} WHERE hasToken(email, 'bachini')",
        use_inverted=is_inverted
    )
    
    # B. Full (Exact) - "henrique.augusto.bachini@hotmail.com"
    full_res = run_query(
        "Full", 
        f"SELECT count() FROM {BENCH_TABLE} WHERE email = 'henrique.augusto.bachini@hotmail.com'",
        use_inverted=is_inverted
    )
    
    results.append({
        "Config": name,
        "Build Time": f"{build_time:.2f}s",
        "Parts": parts,
        "Index Size": idx_size_human,
        
        "Partial ILIKE": f"{partial_res['time']:.2f}s",
        "Partial LIKE": f"{partial_like_res['time']:.2f}s",
        "Token Search": f"{token_res['time']:.2f}s",
        "Full Time": f"{full_res['time']:.2f}s",
        
        "Scanned (ILIKE)": f"{partial_res['read_rows']:,}"
    })

# Final Table
table = Table(title="Benchmark Results (vs Baseline)")
table.add_column("Configuration", style="cyan")
table.add_column("Parts", justify="right")
table.add_column("Index Size", justify="right")
table.add_column("Partial ILIKE", style="magenta")
table.add_column("Partial LIKE", style="magenta")
table.add_column("Token Search", style="yellow")
table.add_column("Full Search", style="green")
table.add_column("Scanned (ILIKE)", style="dim")

# Add Baseline Row
table.add_row(
    "BASELINE (No Index)",
    "-",
    "-",
    f"{baseline_partial['time']:.2f}s",
    f"{baseline_like['time']:.2f}s",
    f"{baseline_token['time']:.2f}s",
    f"{baseline_full['time']:.2f}s",
    f"{baseline_partial['read_rows']:,}"
)

for r in results:
    table.add_row(
        r["Config"],
        str(r["Parts"]),
        str(r["Index Size"]),
        r["Partial ILIKE"],
        r["Partial LIKE"],
        r["Token Search"],
        r["Full Time"],
        r["Scanned (ILIKE)"]
    )

console.print(table)

# Save to JSON
import json
with open("bench_results.json", "w") as f:
    json.dump(results, f, indent=2)
print("\nResults saved to bench_results.json")

# Cleanup
print(f"Cleaning up benchmark table: {BENCH_TABLE}...")
repo.client.command(f"DROP TABLE IF EXISTS {BENCH_TABLE}")
