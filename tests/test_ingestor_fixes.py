import pytest
from pathlib import Path
import queue
import polars as pl
from leakharvester.services.ingestor import BreachIngestor
from leakharvester.adapters.local_fs import LocalFileSystemAdapter

class MockRepository:
    def __init__(self):
        self.staging_creations = []
        self.dropped_tables = []
        self.partition_swaps = []
        self.inserts = []
        self.columns = ["email", "password", "username", "ip"]

    def create_staging_table(self, staging, target):
        self.staging_creations.append((staging, target))

    def drop_table(self, table):
        self.dropped_tables.append(table)

    def replace_partition(self, target, staging, part_id):
        self.partition_swaps.append((target, staging, part_id))

    def insert_arrow_batch(self, table, name):
        self.inserts.append((name, table))

    def get_columns(self, table):
        return self.columns
    
    def add_column(self, table, col):
        self.columns.append(col)

@pytest.fixture
def temp_dirs(tmp_path):
    raw = tmp_path / "raw"
    staging = tmp_path / "staging"
    quarantine = tmp_path / "quarantine"
    raw.mkdir()
    staging.mkdir()
    quarantine.mkdir()
    return raw, staging, quarantine

def test_ingest_cleanup_on_error_fixed(temp_dirs):
    raw, staging, quarantine = temp_dirs

    csv_path = raw / "broken.csv"
    csv_path.write_text("email,pass\ntest@example.com,secret123") 

    fs = LocalFileSystemAdapter()
    
    class FailingRepo(MockRepository):
        def insert_arrow_batch(self, table, name):
            raise RuntimeError("Database connection lost")

    failing_repo = FailingRepo()
    ingestor = BreachIngestor(failing_repo, fs)

    # Should NOT raise, but catch and cleanup
    ingestor.process_file(csv_path, staging, quarantine)
    
    # Verify cleanup
    assert len(failing_repo.staging_creations) == 1
    assert len(failing_repo.dropped_tables) == 1
    assert len(failing_repo.partition_swaps) == 0

def test_latin1_encoding(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    # Simple Latin-1 test
    file_path = raw / "latin1.txt"
    # "email:pass" header, then a row with latin-1 char in PASSWORD
    # This verifies decoding works without tripping email regex
    with open(file_path, "wb") as f:
        f.write(b"email:pass\nuser@example.com:secr\xe9t\n")
        
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    ingestor.process_file(file_path, staging, quarantine)
    
    # Should succeed
    assert len(repo.inserts) == 1
    df = pl.from_arrow(repo.inserts[0][1])
    # Check password has the char (utf8-lossy replaces \xe9 with \ufffd)
    passwords = df["password"].to_list()
    # \ufffd is replacement char
    assert "secr\ufffdt" in passwords

def test_ambiguity_abort(temp_dirs):
    raw, staging, quarantine = temp_dirs
    
    # 3 columns, unknown headers -> Ambiguous
    file_path = raw / "ambiguous.txt"
    file_path.write_text("user:pass:ip\nu1:p1:1.1.1.1")
    
    repo = MockRepository()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    
    ingestor.process_file(file_path, staging, quarantine)
    
    # Should abort (no inserts, no swap)
    assert len(repo.inserts) == 0
    assert len(repo.partition_swaps) == 0
    # Should drop staging table
    assert len(repo.dropped_tables) >= 1