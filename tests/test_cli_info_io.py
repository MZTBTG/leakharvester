from typer.testing import CliRunner
from leakharvester.cli.main import app
from unittest.mock import patch, MagicMock
import pytest

runner = CliRunner()

@patch("leakharvester.cli.commands.info.ClickHouseAdapter")
def test_info(mock_repo_cls):
    """Test 'info' command."""
    mock_repo = mock_repo_cls.return_value
    mock_repo.get_table_stats.return_value = {
        "total_rows": 1000, "compressed_size": "1MB", 
        "uncompressed_size": "5MB", "compression_ratio": "5.0"
    }
    mock_repo.get_columns.return_value = ["col1"]
    mock_repo.get_indices.return_value = []
    mock_repo.get_source_file_stats.return_value = []
    
    result = runner.invoke(app, ["info"])
    
    assert result.exit_code == 0
    assert "Total Records:" in result.output
    assert "1,000" in result.output

@patch("leakharvester.cli.commands.secure_io.SecureIO")
@patch("leakharvester.cli.commands.secure_io.ClickHouseAdapter")
def test_export(mock_repo_cls, mock_secure_io_cls):
    """Test 'export' command."""
    # Mock ClickHouse streaming
    mock_repo = mock_repo_cls.return_value
    mock_repo.get_columns.return_value = ["col1"]
    mock_repo.client.query_arrow_stream.return_value.__enter__.return_value = MagicMock()
    
    result = runner.invoke(app, ["export", "--output", "test.lh", "--no-pass"])
    
    assert result.exit_code == 0
    mock_secure_io_cls.export_data.assert_called()

@patch("leakharvester.cli.commands.secure_io.SecureIO")
@patch("leakharvester.cli.commands.secure_io.ClickHouseAdapter")
def test_import(mock_repo_cls, mock_secure_io_cls):
    """Test 'import' command."""
    mock_secure_io_cls.import_data.return_value = [] # Empty stream
    result = runner.invoke(app, ["import", "--input-file", "test.lh"])
    
    assert result.exit_code == 0
    mock_secure_io_cls.import_data.assert_called()