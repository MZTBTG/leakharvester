import pytest
from unittest.mock import MagicMock
import polars as pl
import threading
import queue
from leakharvester.services.ingestor import BreachIngestor

def test_process_stream_success(tmp_path):
    # Setup
    staging_dir = tmp_path / "staging"
    quarantine_dir = tmp_path / "quarantine"
    staging_dir.mkdir()
    quarantine_dir.mkdir()
    
    mock_repo = MagicMock()
    mock_repo.get_columns.return_value = ["email", "password", "source_file"]
    mock_repo.create_staging_table.return_value = None
    
    # Mock get_arrow_stream_process to return a valid process mock
    mock_process = MagicMock()
    mock_process.stdin = MagicMock()
    mock_process.communicate.return_value = (b"", b"")
    mock_process.returncode = 0
    mock_repo.get_arrow_stream_process.return_value = mock_process
    
    mock_storage = MagicMock()
    # Mock read_stream_batched to return a dataframe
    df = pl.DataFrame({"raw_line": ["test@example.com:pass123"]})
    mock_storage.read_stream_batched.return_value = [df]
    
    ingestor = BreachIngestor(mock_repo, mock_storage)
    
    # Execute - should not raise AttributeError or NameError
    ingestor.process_stream(
        stream=None,
        staging_dir=staging_dir,
        quarantine_dir=quarantine_dir,
        batch_size=1000,
        format="email:password"
    )
