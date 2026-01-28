from pathlib import Path
from typing import Any
import shutil
import polars as pl
import threading
import queue
import io
from leakharvester.ports.file_storage import FileStorage
from leakharvester.domain.schemas import RAW_CSV_SCHEMA

class LocalFileSystemAdapter(FileStorage):
    def scan_csv(self, path: Path) -> pl.LazyFrame:
        sep = ","
        quote = '"'
        has_header = True
        
        if path.suffix in [".txt", ""] or "part" in path.name:
            sep = "\x1F"
            quote = "\x00"
            has_header = False
        else:
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    first_line = f.readline()
                    if ":" in first_line and "," not in first_line:
                        sep = ":"
                        quote = "\x00"
                    elif ";" in first_line and "," not in first_line:
                        sep = ";"
            except Exception:
                pass

        return pl.scan_csv(
            path,
            separator=sep,
            quote_char=quote,
            has_header=has_header,
            ignore_errors=True,
            truncate_ragged_lines=True,
            infer_schema_length=10000,
            low_memory=True,
            new_columns=["raw_line"] if not has_header and sep == "\x1F" else None
        )
    
    def write_parquet(self, lazy_df: pl.LazyFrame, output_path: Path) -> None:
        lazy_df.sink_parquet(output_path, compression="zstd")

    def read_parquet_batches(self, path: Path, batch_size: int = 500_000) -> Any:
        import pyarrow.parquet as pq
        parquet_file = pq.ParquetFile(path)
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            yield batch

    def read_lines_batched(self, path: Path, batch_size: int = 100_000) -> Any:
        try:
            reader = pl.read_csv_batched(
                path,
                separator="\x1F",
                has_header=False,
                new_columns=["raw_line"],
                batch_size=batch_size,
                quote_char="\x00",
                ignore_errors=True,
                truncate_ragged_lines=True,
                encoding="utf8-lossy",
                low_memory=True
            )
            
            accumulated_dfs = []
            current_rows = 0

            while True:
                batches = reader.next_batches(1) 
                if not batches:
                    break
                
                for df in batches:
                    accumulated_dfs.append(df)
                    current_rows += df.height
                
                if current_rows >= batch_size:
                    yield pl.concat(accumulated_dfs)
                    accumulated_dfs = []
                    current_rows = 0
            
            if accumulated_dfs:
                yield pl.concat(accumulated_dfs)

        except Exception:
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
        target_bytes = batch_size * 100 
        
        chunk_queue = queue.Queue(maxsize=3)
        
        def _reader_thread(r_stream, q, t_bytes):
            try:
                while True:
                    chunk = r_stream.read(t_bytes)
                    if not chunk:
                        q.put(None)
                        break
                    
                    remainder = r_stream.readline()
                    if remainder:
                        chunk += remainder
                    
                    q.put(chunk)
            except Exception:
                q.put(None)

        try:
            raw_stream = getattr(stream, 'buffer', stream)
            
            t = threading.Thread(target=_reader_thread, args=(raw_stream, chunk_queue, target_bytes), daemon=True)
            t.start()
            
            while True:
                chunk = chunk_queue.get()
                if chunk is None:
                    break
                
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
                        low_memory=True,
                        n_threads=None
                    )
                    
                    yield df
                    
                except pl.exceptions.NoDataError:
                    pass
                finally:
                    del chunk
                    del buffer
                
        except Exception:
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
