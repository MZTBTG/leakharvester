from typer.testing import CliRunner
from leakharvester.cli.main import app
from unittest.mock import patch, MagicMock
from pathlib import Path
import pytest

runner = CliRunner()

@patch("leakharvester.cli.commands.ingest.BreachIngestor")
@patch("leakharvester.cli.commands.ingest.LocalFileSystemAdapter")
@patch("leakharvester.cli.commands.ingest.ClickHouseAdapter")
def test_ingest_file(mock_repo_cls, mock_fs_cls, mock_ingestor_cls):
    """Test 'ingest --file'."""
    mock_ingestor = mock_ingestor_cls.return_value
    
    with runner.isolated_filesystem():
        with open("test_leak.txt", "w") as f:
            f.write("data")
        
        result = runner.invoke(app, ["ingest", "--file", "test_leak.txt"])
        
        assert result.exit_code == 0
        mock_ingestor.process_file.assert_called()

@patch("leakharvester.cli.commands.ingest.BreachIngestor")
@patch("leakharvester.cli.commands.ingest.LocalFileSystemAdapter")
@patch("leakharvester.cli.commands.ingest.ClickHouseAdapter")
def test_ingest_stdin(mock_repo_cls, mock_fs_cls, mock_ingestor_cls):
    """Test 'ingest --stdin'."""
    result = runner.invoke(app, ["ingest", "--stdin"], input="some data")
    
    assert result.exit_code == 0
    mock_ingestor_cls.return_value.process_stream.assert_called()