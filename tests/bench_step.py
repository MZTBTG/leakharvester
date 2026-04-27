import sys
import os
import subprocess
import time
import uuid
import traceback
import argparse
from rich.console import Console

# 1. Fix Import Path
sys.path.append(os.path.abspath("src"))

from leakharvester.adapters.clickhouse import ClickHouseAdapter

console = Console()
repo = ClickHouseAdapter()

BENCH_TABLE = "vault.bench_breach_records"
PROD_TABLE = "vault.breach_records"

configs = [
    ("Eco Ngram", ["--ngram"]),
    ("Turbo Ngram", ["--ngram", "--ngram-size", "131072"]),
    ("Eco Token", ["--tokenbf"]),
    ("Turbo Token", ["--tokenbf", "--tokenbf-size", "131072"]),
    ("Inverted", ["--inverted"])
]

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

def run_query(name, query_sql, ignore_index=False, use_inverted=False, warmup=0, iterations=1):
    base_query_id = f"bench_{uuid.uuid4().hex}"
    # High precision timer
    
    # Force max resources
    settings = {
        'max_threads': 20, 
        'max_memory_usage': 40000000000, 
        'use_query_cache': 0 # Disable cache for accurate bench
    }
    
    if ignore_index:
        settings['ignore_data_skipping_indices'] = 1 
        settings['use_skip_indexes'] = 0
        
    if use_inverted:
        settings['allow_experimental_inverted_index'] = 1

    try:
        # Warmup
        for _ in range(warmup):
            repo.client.query(query_sql, settings=settings)

        # Iterations
        total_time = 0
        last_query_id = ""
        
        for i in range(iterations):
            query_id = f"{base_query_id}_{i}"
            settings['query_id'] = query_id
            
            start = time.perf_counter()
            repo.client.query(query_sql, settings=settings)
            total_time += (time.perf_counter() - start)
            last_query_id = query_id
        
        avg_time = total_time / iterations
        
        # Get detailed metrics
        read_rows, read_bytes, ram = get_query_metrics(last_query_id)
        
        return {
            "time": avg_time,
            "read_rows": read_rows,
            "read_bytes": read_bytes
        }
    except Exception as e:
        print(f"\n[CRITICAL] Query '{name}' Failed!")
        print(f"SQL: {query_sql}")
        print(f"Settings: {settings}")
        print("-" * 60)
        traceback.print_exc()
        raise e

def run_step(step_idx, warmup=0, iterations=1):
    if step_idx < 0 or step_idx >= len(configs):
        print(f"Invalid step index: {step_idx}")
        return

    # Setup Benchmark Table
    print(f"Setting up benchmark table: {BENCH_TABLE}...")
    try:
        repo.client.command(f"DROP TABLE IF EXISTS {BENCH_TABLE}")
        repo.client.command(f"CREATE TABLE {BENCH_TABLE} AS {PROD_TABLE}")
        repo.client.command(f"INSERT INTO {BENCH_TABLE} SELECT * FROM {PROD_TABLE} LIMIT 1000000")
    except Exception as e:
        print(f"[CRITICAL] Failed to setup benchmark table: {e}")
        return

    name, args = configs[step_idx]
    console.rule(f"Testing Configuration [{step_idx}]: {name}")
    
    # 0. Kill Pending Mutations
    # We do this here too just to be safe between steps if run manually
    repo.client.command(f"KILL MUTATION WHERE table='{BENCH_TABLE.split('.')[1]}' AND is_done=0")

    # 1. Switch Mode (Build Index)
    start_build = time.perf_counter()
    
    print("Removing previous indexes...")
    subprocess.run(["uv", "run", "python", "-m", "leakharvester.cli.main", "index", "-c", "email", "--remove", "--yes", "--table", BENCH_TABLE], check=True)
    
    cmd = ["uv", "run", "python", "-m", "leakharvester.cli.main", "index", "-c", "email", "--table", BENCH_TABLE] + args
        
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    
    # 2. Optimize (Reduce Chunks)
    # Skipped to avoid timeout on large dataset
    print("Skipping OPTIMIZE TABLE FINAL (Chunk reduction) to avoid timeout...")
    # print("Forcefully merging data parts (OPTIMIZE FINAL)...")
    # try:
    #     repo.client.command(f"OPTIMIZE TABLE {BENCH_TABLE} FINAL", settings={'receive_timeout': 3600})
    # except Exception as e:
    #     print("[CRITICAL] OPTIMIZE TABLE Failed!")
    #     traceback.print_exc()
    #     raise e
        
    build_time = time.perf_counter() - start_build
    
    # 3. Stats
    parts, idx_size_human, idx_size_bytes = get_stats()
    
    # 4. Search Tests
    is_inverted = ("Inverted" in name)
    
    partial_res = run_query(
        "Partial ILIKE", 
        f"SELECT count() FROM {BENCH_TABLE} WHERE email ILIKE '%augusto.bachini%'",
        use_inverted=is_inverted,
        warmup=warmup,
        iterations=iterations
    )
    
    partial_like_res = run_query(
        "Partial LIKE", 
        f"SELECT count() FROM {BENCH_TABLE} WHERE email LIKE '%augusto.bachini%'",
        use_inverted=is_inverted,
        warmup=warmup,
        iterations=iterations
    )

    token_res = run_query(
        "Token Search", 
        f"SELECT count() FROM {BENCH_TABLE} WHERE hasToken(email, 'bachini')",
        use_inverted=is_inverted,
        warmup=warmup,
        iterations=iterations
    )
    
    full_res = run_query(
        "Full", 
        f"SELECT count() FROM {BENCH_TABLE} WHERE email = 'henrique.augusto.bachini@hotmail.com'",
        use_inverted=is_inverted,
        warmup=warmup,
        iterations=iterations
    )
    
    # Print simplified result line for easy parsing
    print("\n--- RESULT ---")
    print(f"CONFIG:{name}|BUILD:{build_time:.2f}|PARTS:{parts}|SIZE:{idx_size_human}|P_ILIKE:{partial_res['time']:.4f}|P_LIKE:{partial_like_res['time']:.4f}|TOKEN:{token_res['time']:.4f}|FULL:{full_res['time']:.4f}|SCAN:{partial_res['read_rows']}")
    print("--- END RESULT ---")
    
    # Teardown
    print(f"Cleaning up {BENCH_TABLE}...")
    repo.client.command(f"DROP TABLE IF EXISTS {BENCH_TABLE}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("step", type=int, help="Step index (0-4)")
    parser.add_argument("--warmup", type=int, default=0, help="Number of warmup runs")
    parser.add_argument("--iter", type=int, default=1, help="Number of iterations")
    args = parser.parse_args()
    run_step(args.step, warmup=args.warmup, iterations=args.iter)
