from typer.testing import CliRunner
from leakharvester.cli.main import app
from unittest.mock import patch, MagicMock
import pytest

runner = CliRunner()

@patch("leakharvester.cli.commands.repair.ClickHouseAdapter")
@patch("leakharvester.cli.commands.repair.LocalFileSystemAdapter")
@patch("leakharvester.cli.commands.repair.BreachIngestor")
def test_repair(mock_ingestor_cls, mock_fs_cls, mock_repo_cls):
    """Test 'repair' command."""
    result = runner.invoke(app, ["repair"])
    
    assert result.exit_code == 0
    mock_ingestor_cls.return_value.repair_quarantine.assert_called()