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

configs = [
    ("Eco Ngram", "eco", "ngram"),
    ("Turbo Ngram", "turbo", "ngram"),
    ("Eco Token", "eco", "token"),
    ("Turbo Token", "turbo", "token"),
    ("Inverted", "turbo", "inverted")
]

def get_stats():
    # Get Part Count (Chunks)
    parts = repo.client.query("SELECT count() FROM system.parts WHERE table='breach_records' AND active=1").result_rows[0][0]
    
    # Get Index Size (Bytes on disk)
    try:
        size_bytes = repo.client.query("SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE table='breach_records' AND active=1").result_rows[0][0]
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
        settings['ignore_data_skipping_indices'] = 1 
        settings['use_skip_indexes'] = 0
        
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
        print(f"\n[CRITICAL] Query '{name}' Failed!")
        print(f"SQL: {query_sql}")
        print(f"Settings: {settings}")
        print("-" * 60)
        traceback.print_exc()
        raise e

def run_step(step_idx):
    if step_idx < 0 or step_idx >= len(configs):
        print(f"Invalid step index: {step_idx}")
        return

    name, mode, type_arg = configs[step_idx]
    console.rule(f"Testing Configuration [{step_idx}]: {name}")
    
    # 0. Kill Pending Mutations
    # We do this here too just to be safe between steps if run manually
    repo.client.command("KILL MUTATION WHERE table='breach_records' AND is_done=0")

    # 1. Switch Mode (Build Index)
    start_build = time.perf_counter()
    cmd = ["uv", "run", "python", "-m", "leakharvester.main", "switch-mode", mode]
    
    if type_arg == "inverted":
        cmd.append("--inverted-index")
    else:
        cmd.extend(["--token-type", type_arg])
        
    print(f"Running command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    
    # 2. Optimize (Reduce Chunks)
    # Skipped to avoid timeout on large dataset
    print("Skipping OPTIMIZE TABLE FINAL (Chunk reduction) to avoid timeout...")
    # print("Forcefully merging data parts (OPTIMIZE FINAL)...")
    # try:
    #     repo.client.command("OPTIMIZE TABLE vault.breach_records FINAL", settings={'receive_timeout': 3600})
    # except Exception as e:
    #     print("[CRITICAL] OPTIMIZE TABLE Failed!")
    #     traceback.print_exc()
    #     raise e
        
    build_time = time.perf_counter() - start_build
    
    # 3. Stats
    parts, idx_size_human, idx_size_bytes = get_stats()
    
    # 4. Search Tests
    is_inverted = (type_arg == "inverted")
    
    partial_res = run_query(
        "Partial ILIKE", 
        "SELECT count() FROM vault.breach_records WHERE email ILIKE '%augusto.bachini%'",
        use_inverted=is_inverted
    )
    
    partial_like_res = run_query(
        "Partial LIKE", 
        "SELECT count() FROM vault.breach_records WHERE email LIKE '%augusto.bachini%'",
        use_inverted=is_inverted
    )

    token_res = run_query(
        "Token Search", 
        "SELECT count() FROM vault.breach_records WHERE hasToken(email, 'bachini')",
        use_inverted=is_inverted
    )
    
    full_res = run_query(
        "Full", 
        "SELECT count() FROM vault.breach_records WHERE email = 'henrique.augusto.bachini@hotmail.com'",
        use_inverted=is_inverted
    )
    
    # Print simplified result line for easy parsing
    print("\n--- RESULT ---")
    print(f"CONFIG:{name}|BUILD:{build_time:.2f}|PARTS:{parts}|SIZE:{idx_size_human}|P_ILIKE:{partial_res['time']:.4f}|P_LIKE:{partial_like_res['time']:.4f}|TOKEN:{token_res['time']:.4f}|FULL:{full_res['time']:.4f}|SCAN:{partial_res['read_rows']}")
    print("--- END RESULT ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("step", type=int, help="Step index (0-4)")
    args = parser.parse_args()
    run_step(args.step)
