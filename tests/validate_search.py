import time
import json
import clickhouse_connect
from leakharvester.config import settings

def run_tests():
    client = clickhouse_connect.get_client(
        host=settings.CLICKHOUSE_HOST,
        port=settings.CLICKHOUSE_PORT,
        username=settings.CLICKHOUSE_USER,
        password=settings.CLICKHOUSE_PASSWORD,
        database=settings.CLICKHOUSE_DB
    )
    
    queries = [
        {"name": "Password Search", "sql": "SELECT * FROM vault.breach_records WHERE password = '365500' LIMIT 5"},
        {"name": "Exact Email", "sql": "SELECT * FROM vault.breach_records WHERE email = 'henrique.augusto.bachini@hotmail.com'"},
        {"name": "Substring Email", "sql": "SELECT * FROM vault.breach_records WHERE email LIKE '%augusto.bachini%' LIMIT 5"},
        {"name": "Token Search (Global)", "sql": "SELECT * FROM vault.breach_records WHERE hasToken(_search_blob, 'bachini') LIMIT 5"}
    ]
    
    results = []
    
    for q in queries:
        print(f"Running: {q['name']}...")
        start = time.time()
        # Enable profile events to get scan metrics
        res = client.query(q['sql'], settings={"log_queries": 1}) 
        elapsed = time.time() - start
        
        # We can't easily get 'rows_read' from client.query object directly in simple mode 
        # without querying system.query_log, but timing is the main metric here.
        # We will save the result count and time.
        
        results.append({
            "test": q['name'],
            "elapsed_seconds": elapsed,
            "result_count": res.row_count,
            "data": res.result_rows
        })
        
    with open("tests/search_results.json", "w") as f:
        json.dump(results, f, indent=4, default=str)
        
    print("Tests complete. Results saved.")

if __name__ == "__main__":
    run_tests()
