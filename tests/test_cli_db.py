from typer.testing import CliRunner
from leakharvester.cli.main import app
from unittest.mock import patch, MagicMock
import pytest

runner = CliRunner()

def test_db_help_command():
    """Test 'db --help' command to verify registration."""
    result = runner.invoke(app, ["db", "--help"])
    assert result.exit_code == 0
    assert "Database Lifecycle Management" in result.output

@patch("leakharvester.cli.commands.db.SettingsManager")
@patch("leakharvester.cli.commands.db.ensure_db_running")
@patch("leakharvester.cli.commands.db.ClickHouseAdapter")
def test_db_status_command_mocked(mock_repo_cls, mock_ensure, mock_sm_cls):
    """Test 'db --status' with mocks."""
    # Mock SettingsManager
    mock_sm_instance = mock_sm_cls.return_value
    mock_sm_instance.get_active_db_path.return_value = "/tmp/test_db"
    
    # Mock ClickHouseAdapter
    mock_repo_instance = mock_repo_cls.return_value
    mock_repo_instance.get_table_stats.return_value = {"total_rows": 100}

    from leakharvester.cli.main import app
    result = runner.invoke(app, ["db", "--status"])
    
    assert result.exit_code == 0
    assert "Active Configured Path:" in result.output
    assert "Database is Online" in result.output
    assert "Records: 100" in result.output
    
    # Verify calls
    mock_ensure.assert_called_once()
    mock_repo_cls.assert_called_once()
    mock_repo_instance.get_table_stats.assert_called_with("vault.breach_records")

@patch("leakharvester.cli.commands.db.SettingsManager")
@patch("leakharvester.cli.commands.db.ensure_db_running")
@patch("leakharvester.cli.commands.db.ClickHouseAdapter")
@patch("leakharvester.cli.commands.db.Confirm")
def test_db_init_command(mock_confirm, mock_repo_cls, mock_ensure, mock_sm_cls):
    """Test 'db --init'."""
    mock_sm_instance = mock_sm_cls.return_value
    mock_sm_instance.get_active_db_path.return_value = "/tmp/test_db"
    
    result = runner.invoke(app, ["db", "--init"])
    
    assert result.exit_code == 0
    mock_ensure.assert_called_once()
    mock_repo_cls.return_value.execute_ddl.assert_called()