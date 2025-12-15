import pytest
import polars as pl
import pyarrow as pa
from pathlib import Path
from leakharvester.services.ingestor import BreachIngestor
from leakharvester.adapters.local_fs import LocalFileSystemAdapter

class MockRepository:
    def __init__(self):
        self.batches = []
        self.ddl_calls = []

    def insert_arrow_batch(self, table: pa.Table, table_name: str) -> None:
        self.batches.append((table, table_name))

    def execute_ddl(self, ddl_statement: str) -> None:
        self.ddl_calls.append(ddl_statement)

def test_ingest_flow_valid_file(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    # Create sample CSV
    csv_path = raw / "test_leak.csv"
    csv_content = "email,username,password\ntest@example.com,user1,pass1\ntest2@example.com,user2,pass2"
    csv_path.write_text(csv_content)
    
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    ingestor.process_file(csv_path, staging, quarantine)
    
    # Verify staging file created
    assert (staging / "test_leak.parquet").exists()
    
    # Verify repository received data
    assert len(repo.batches) > 0
    table, name = repo.batches[0]
    assert name == "breach_records"
    assert table.num_rows == 2
    
    # Verify content via Polars
    # We can read the parquet file back to check normalization
    df = pl.read_parquet(staging / "test_leak.parquet")
    assert "source_file" in df.columns
    assert df["source_file"][0] == "test_leak.csv"
    assert "_search_blob" in df.columns
    assert "test@example.com" in df["_search_blob"][0]

def test_ingest_flow_dirty_columns(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    # Create sample CSV with weird headers
    csv_path = raw / "dirty.csv"
    csv_content = "E-Mail,Login Name,Pwd\ndirty@example.com,dirtyuser,dirtypass"
    csv_path.write_text(csv_content)
    
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    ingestor.process_file(csv_path, staging, quarantine)
    
    # Verify it was mapped correctly
    df = pl.read_parquet(staging / "dirty.parquet")
    assert "email" in df.columns
    assert "username" in df.columns
    assert "password" in df.columns
    assert df["email"][0] == "dirty@example.com"

def test_ingest_flow_quarantine(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    # Create sample CSV that cannot be mapped
    csv_path = raw / "unmappable.csv"
    csv_content = "Col1,Col2,Col3\nVal1,Val2,Val3"
    csv_path.write_text(csv_content)
    
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    ingestor.process_file(csv_path, staging, quarantine)
    
    # Verify moved to quarantine
    assert (quarantine / "unmappable.csv").exists()
    assert not (raw / "unmappable.csv").exists()
