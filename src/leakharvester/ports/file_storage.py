from typing import Protocol
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
    
    def move_file(self, src: Path, dest: Path) -> None:
        """Moves a file from src to dest."""
        ...
