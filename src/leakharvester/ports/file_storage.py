from typing import Protocol, Any
from pathlib import Path
import polars as pl

class FileStorage(Protocol):
    def scan_csv(self, path: Path) -> pl.LazyFrame:
        """Lazily scans a CSV file."""
        ...
    
    def write_parquet(self, lazy_df: pl.LazyFrame, output_path: Path) -> None:
        """Writes a LazyFrame to a Parquet file."""
        ...

    def read_parquet_batches(self, path: Path, batch_size: int = 500_000) -> Any:
        """Yields PyArrow batches from a Parquet file."""
        ...
    
    def read_lines_batched(self, path: Path, batch_size: int = 100_000) -> Any:
        """Yields batches of raw lines from a text file."""
        ...

    def read_stream_batched(self, stream: Any, batch_size: int = 100_000) -> Any:
        """Yields batches of raw lines from a file-like stream (stdin)."""
        ...

    def move_file(self, src: Path, dest: Path) -> None:
        """Moves a file from src to dest."""
        ...
