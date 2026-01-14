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
        self.staging_creations = []
        self.partition_swaps = []
        self.dropped_tables = []
        self.columns = ["email", "username", "password", "source_file", "breach_date", "import_date"]

    def insert_arrow_batch(self, table: pa.Table, table_name: str) -> None:
        self.batches.append((table, table_name))

    def execute_ddl(self, ddl_statement: str) -> None:
        self.ddl_calls.append(ddl_statement)
    
    def create_staging_table(self, staging_table: str, source_table: str) -> None:
        self.staging_creations.append((staging_table, source_table))
    
    def drop_table(self, table_name: str) -> None:
        self.dropped_tables.append(table_name)
    
    def replace_partition(self, target_table: str, staging_table: str, partition_id: str) -> None:
        self.partition_swaps.append((target_table, staging_table, partition_id))
    
    def get_columns(self, table_name: str) -> list[str]:
        return self.columns
    
    def add_column(self, table_name: str, column_name: str) -> None:
        self.columns.append(column_name)

def test_custom_source_name(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    # Create sample CSV
    csv_path = raw / "generic_leak.csv"
    csv_content = "email,password\nuser@test.com,pass123"
    csv_path.write_text(csv_content)
    
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    # Process with custom source name (Default: Idempotent)
    ingestor.process_file(
        csv_path, 
        staging, 
        quarantine, 
        custom_source_name="CustomDB_2024"
    )
    
    # Should write to staging table
    assert len(repo.staging_creations) == 1
    staging_table = repo.staging_creations[0][0]
    
    assert len(repo.batches) > 0
    table, table_name = repo.batches[0]
    assert table_name == staging_table
    
    df = pl.from_arrow(table)
    
    assert "source_file" in df.columns
    assert df["source_file"][0] == "CustomDB_2024"
    
    # Should swap partition using custom name
    assert len(repo.partition_swaps) == 1
    target, staging, part_id = repo.partition_swaps[0]
    assert part_id == "CustomDB_2024"

def test_idempotency_flow(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    csv_path = raw / "test_leak.csv"
    csv_content = "email,username,password\ntest@example.com,user1,pass1"
    csv_path.write_text(csv_content)
    
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    # Ingest (Default = Idempotent)
    ingestor.process_file(csv_path, staging, quarantine)
    
    # Verify staging table creation
    assert len(repo.staging_creations) == 1
    staging_table_name = repo.staging_creations[0][0]
    assert staging_table_name.startswith("vault.staging_")
    
    # Verify insert to staging
    assert len(repo.batches) == 1
    assert repo.batches[0][1] == staging_table_name
    
    # Verify Replace Partition
    assert len(repo.partition_swaps) == 1
    target, source_stage, partition = repo.partition_swaps[0]
    assert target == "vault.breach_records"
    assert source_stage == staging_table_name
    assert partition == "test_leak.csv"
    
    # Verify Drop Staging
    assert len(repo.dropped_tables) == 1
    assert repo.dropped_tables[0] == staging_table_name

def test_append_mode(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    csv_path = raw / "append_test.csv"
    csv_content = "email,password\nuser1@test.com,pass1"
    csv_path.write_text(csv_content)
    
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    # Ingest with Append = True
    ingestor.process_file(csv_path, staging, quarantine, append=True)
    
    # Should NOT create staging
    assert len(repo.staging_creations) == 0
    
    # Should insert directly to main table
    assert len(repo.batches) == 1
    table, table_name = repo.batches[0]
    assert table_name == "vault.breach_records"
    
    # Should NOT swap partition
    assert len(repo.partition_swaps) == 0
    
    # Should NOT drop table
    assert len(repo.dropped_tables) == 0

def test_dirty_columns_idempotent(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    csv_path = raw / "dirty.csv"
    csv_content = "E-Mail,Login Name,Pwd\ndirty@example.com,dirtyuser,dirtypass"
    csv_path.write_text(csv_content)
    
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    ingestor.process_file(csv_path, staging, quarantine)
    
    # Should still use staging
    assert len(repo.staging_creations) == 1
    
    df = pl.from_arrow(repo.batches[0][0])
    assert "email" in df.columns
    assert df["email"][0] == "dirty@example.com"

def test_ingest_cleanup_on_error(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    csv_path = raw / "broken.csv"
    csv_path.write_text("email,pass\ntest@example.com,val") # Valid content
    
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    # Simulate error during processing by mocking repository to fail on insert
    class FailingRepo(MockRepository):
        def insert_arrow_batch(self, table, name):
            raise RuntimeError("Database connection lost")
    
    failing_repo = FailingRepo()
    ingestor = BreachIngestor(failing_repo, fs)
    
    # process_file catches the error and moves to quarantine, so it should NOT raise.
    ingestor.process_file(csv_path, staging, quarantine)
    
    # Should have attempted to create staging
    assert len(failing_repo.staging_creations) == 1
    staging_table = failing_repo.staging_creations[0][0]
    
    # Should have attempted to drop staging (cleanup)
    assert len(failing_repo.dropped_tables) == 1
    assert failing_repo.dropped_tables[0] == staging_table
    
    # Should NOT have swapped partition
    assert len(failing_repo.partition_swaps) == 0
    
    # Verify moved to quarantine (This confirms the error path was taken)
    assert (quarantine / "broken.csv").exists()
    assert not (raw / "broken.csv").exists()
