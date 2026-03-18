import pytest
import polars as pl
from leakharvester.services.ingestor import BreachIngestor
from leakharvester.adapters.local_fs import LocalFileSystemAdapter

import io
import pyarrow as pa

class MockStdin(io.BytesIO):
    def close(self):
        pass

class MockProcess:
    def __init__(self, repo, table_name):
        self.mock_stdin = MockStdin()
        self.stdin = self.mock_stdin
        self.repo = repo
        self.table_name = table_name
        self.returncode = 0

    def communicate(self):
        self.mock_stdin.seek(0)
        try:
            reader = pa.ipc.open_stream(self.mock_stdin)
            while True:
                try:
                    batch = reader.read_next_batch()
                    table = pa.Table.from_batches([batch])
                    self.repo.inserts.append((self.table_name, table))
                except StopIteration:
                    break
        except Exception:
            pass
        return (b"", b"")

    def kill(self):
        pass

    def poll(self):
        return self.returncode

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

    def add_column(self, table, name, type_):
        self.columns.append(name)

    def get_arrow_stream_process(self, table_name, columns=None):
        return MockProcess(self, table_name)

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
    csv_path.write_text("email,password\ntest@example.com,secret123") 

    fs = LocalFileSystemAdapter()
    
    class FailingRepo(MockRepository):
        def get_arrow_stream_process(self, table_name, columns=None):
            p = MockProcess(self, table_name)
            p.returncode = 1
            # Mock communicate to simulate a failure
            def mock_communicate():
                return (b"", b"Simulated database connection lost")
            p.communicate = mock_communicate
            return p

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
    # "email:password" header, then a row with latin-1 char in PASSWORD
    # This verifies decoding works without tripping email regex
    with open(file_path, "wb") as f:
        f.write(b"email:password\nuser@example.com:secr\xe9t\n")
        
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