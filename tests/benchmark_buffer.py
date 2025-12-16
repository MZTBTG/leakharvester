import polars as pl
import io
import time
import threading

# Generate dummy CSV data (approx 500MB)
print("Generating dummy data...")
row_count = 5_000_000
data = b"email,password\n" + b"test@example.com,password123\n" * row_count
print(f"Data size: {len(data) / 1024 / 1024:.2f} MB")

def benchmark_stream():
    # Simulate non-seekable stream
    stream = io.BytesIO(data)
    stream.seekable = lambda: False # Monkeypatch to force stream mode behavior
    
    start = time.time()
    reader = pl.read_csv_batched(stream, batch_size=1_000_000, separator=",")
    count = 0
    while True:
        batches = reader.next_batches(1)
        if not batches: break
        count += batches[0].height
    
    end = time.time()
    print(f"Stream Mode: {end - start:.4f}s ({count} rows)")

def benchmark_buffered():
    # Seekable BytesIO (simulating buffered chunk)
    buffer = io.BytesIO(data)
    
    start = time.time()
    # Read entire buffer at once (Parallel)
    df = pl.read_csv(buffer, separator=",")
    count = df.height
    
    end = time.time()
    print(f"Buffered Mode: {end - start:.4f}s ({count} rows)")

print("\n--- Benchmarking ---")
benchmark_stream()
benchmark_buffered()
