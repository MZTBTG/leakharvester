from pathlib import Path
import shutil
import polars as pl
from leakharvester.ports.file_storage import FileStorage
from leakharvester.domain.schemas import RAW_CSV_SCHEMA

class LocalFileSystemAdapter(FileStorage):
    def scan_csv(self, path: Path) -> pl.LazyFrame:
        return pl.scan_csv(
            path,
            separator=",", # Ideally auto-detect, but fixed for now per doc
            ignore_errors=True,
            infer_schema_length=10000,
            low_memory=True,
            # We enforce a relaxed schema initially to avoid early crashing
            # But pl.scan_csv with schema_overrides is tricky if columns don't match names yet.
            # So we might scan without schema first or let Polars infer.
            # The doc example uses schema_overrides.
            # Let's rely on inference for now as column names are unknown before mapping.
        )
    
    def write_parquet(self, lazy_df: pl.LazyFrame, output_path: Path) -> None:
        # sink_parquet executes the lazy graph
        lazy_df.sink_parquet(output_path, compression="zstd")

    def read_parquet_batches(self, path: Path, batch_size: int = 500_000) -> Any:
        import pyarrow.parquet as pq
        parquet_file = pq.ParquetFile(path)
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            yield batch

    def move_file(self, src: Path, dest: Path) -> None:
        shutil.move(str(src), str(dest))
