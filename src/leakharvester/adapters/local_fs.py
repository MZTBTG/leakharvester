from pathlib import Path
from typing import Any
import shutil
import polars as pl
from leakharvester.ports.file_storage import FileStorage
from leakharvester.domain.schemas import RAW_CSV_SCHEMA

class LocalFileSystemAdapter(FileStorage):
    def scan_csv(self, path: Path) -> pl.LazyFrame:
        # Simple sniffer to detect separator
        sep = ","
        quote = '"'
        has_header = True
        
        # If extension is .txt or looks like raw dump, read as lines
        if path.suffix in [".txt", ""] or "part" in path.name:
            sep = "\x1F" # Unit Separator (unlikely to exist)
            quote = "\x00" # Disable quoting
            has_header = False
        else:
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    first_line = f.readline()
                    if ":" in first_line and "," not in first_line:
                        sep = ":"
                        quote = "\x00" # Often dumps use : without quotes
                    elif ";" in first_line and "," not in first_line:
                        sep = ";"
                    # else default
            except Exception:
                pass

        return pl.scan_csv(
            path,
            separator=sep,
            quote_char=quote,
            has_header=has_header,
            ignore_errors=True,
            truncate_ragged_lines=True, # Critical for mixed dumps
            infer_schema_length=10000,
            low_memory=True,
            new_columns=["raw_line"] if not has_header and sep == "\x1F" else None
        )
    
    def write_parquet(self, lazy_df: pl.LazyFrame, output_path: Path) -> None:
        # sink_parquet executes the lazy graph
        lazy_df.sink_parquet(output_path, compression="zstd")

    def read_parquet_batches(self, path: Path, batch_size: int = 500_000) -> Any:
        import pyarrow.parquet as pq
        parquet_file = pq.ParquetFile(path)
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            yield batch

    def read_lines_batched(self, path: Path, batch_size: int = 100_000) -> Any:
        # Optimized reader using Polars Rust engine
        try:
            reader = pl.read_csv_batched(
                path,
                separator="\x1F", # Unit Separator (Fake)
                has_header=False,
                new_columns=["raw_line"],
                batch_size=batch_size, # Request large batches
                quote_char="\x00",
                ignore_errors=True,
                truncate_ragged_lines=True,
                encoding="utf8-lossy",
                low_memory=True
            )
            
            # Accumulator for true batch size
            accumulated_dfs = []
            current_rows = 0

            while True:
                # Request a batch. Polars might return a list of smaller DFs.
                batches = reader.next_batches(1) 
                if not batches:
                    break
                
                for df in batches:
                    accumulated_dfs.append(df)
                    current_rows += df.height
                
                # If we have enough rows, yield a concatenated large batch
                if current_rows >= batch_size:
                    yield pl.concat(accumulated_dfs)
                    accumulated_dfs = []
                    current_rows = 0
            
            # Yield remaining
            if accumulated_dfs:
                yield pl.concat(accumulated_dfs)

        except Exception:
             # Fallback (unchanged)
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                batch = []
                for line in f:
                    batch.append(line.strip())
                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                if batch:
                    yield batch

    def read_stream_batched(self, stream: Any, batch_size: int = 100_000) -> Any:
        # Optimized "Buffered Parallel" strategy
        # Instead of feeding the stream directly (which forces sequential read),
        # we buffer a large chunk of bytes into RAM (BytesIO) and feed it to pl.read_csv.
        # This tricks Polars into treating it as a "file", enabling multi-threaded parsing.
        
        # Estimate bytes per row to calculate target buffer size
        # Assume 100 bytes per row (conservative for leaks)
        target_bytes = batch_size * 100 
        
        try:
            # If stream has a 'buffer' attribute (like sys.stdin), use it for raw byte access
            raw_stream = getattr(stream, 'buffer', stream)
            
            while True:
                # Read a large chunk
                chunk = raw_stream.read(target_bytes)
                if not chunk:
                    break
                
                # We likely stopped in the middle of a line. Read until newline.
                remainder = raw_stream.readline()
                if remainder:
                    chunk += remainder
                
                # Wrap in BytesIO to make it seekable for Polars
                import io
                buffer = io.BytesIO(chunk)
                
                try:
                    df = pl.read_csv(
                        buffer,
                        separator="\x1F",
                        has_header=False,
                        new_columns=["raw_line"],
                        quote_char="\x00",
                        ignore_errors=True,
                        truncate_ragged_lines=True,
                        encoding="utf8-lossy",
                        low_memory=True, # Critical to avoid RAM spike during parse
                        n_threads=None # Use all available threads
                    )
                    
                    yield df
                    
                except pl.exceptions.NoDataError:
                    pass
                
        except Exception:
            # Fallback to line-by-line if binary read fails
            batch = []
            try:
                for line in stream:
                    if isinstance(line, bytes):
                        line = line.decode("utf-8", errors="ignore")
                    batch.append(line.strip())
                    if len(batch) >= batch_size:
                        yield pl.DataFrame({"raw_line": batch})
                        batch = []
                if batch:
                    yield pl.DataFrame({"raw_line": batch})
            except Exception:
                pass

    def move_file(self, src: Path, dest: Path) -> None:
        shutil.move(str(src), str(dest))
