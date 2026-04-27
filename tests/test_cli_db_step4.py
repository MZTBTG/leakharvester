import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path
from leakharvester.cli.commands.db import db_command
import typer

@pytest.fixture
def mock_dependencies():
    with patch("leakharvester.cli.commands.db.SettingsManager") as sm_mock, \
         patch("leakharvester.cli.commands.db.ClickHouseAdapter") as ch_mock, \
         patch("leakharvester.cli.commands.db.Console") as console_mock, \
         patch("leakharvester.cli.commands.db.Confirm") as confirm_mock, \
         patch("leakharvester.cli.commands.db.ensure_db_running") as ensure_mock:
        
        yield {
            "sm": sm_mock.return_value,
            "ch": ch_mock.return_value,
            "console": console_mock.return_value,
            "confirm": confirm_mock,
            "ensure_db": ensure_mock
        }

def test_rmfile_no_valid_filenames(mock_dependencies):
    """Test passing empty string or whitespace."""
    with patch("leakharvester.cli.commands.db.log_error") as mock_log_error:
        db_command(rmfile="  ,  ", path=None, init=False, compression=3, status=False, lsfiles=False, allfiles=False, remove=False, reset_all=False, verbose=False)
        
        # Verify no DB calls
        mock_dependencies["ch"].client.command.assert_not_called()
        mock_log_error.assert_called_with("No valid filenames provided.")

def test_rmfile_invalid_file(mock_dependencies):
    """Test passing a file that doesn't exist in DB."""
    deps = mock_dependencies
    
    # Define side effect for query
    def query_side_effect(sql, *args, **kwargs):
        mock_res = MagicMock()
        if "SELECT DISTINCT" in sql:
             mock_res.result_rows = [("valid.txt",)]
        else:
             # The list query
             mock_res.result_rows = [("valid.txt", 100, "2023-01-01", "2023-01-02")]
        return mock_res

    deps["ch"].client.query.side_effect = query_side_effect
    
    with patch("leakharvester.cli.commands.db.log_error") as mock_log_error:
        db_command(rmfile="invalid.txt", path=None, init=False, compression=3, status=False, lsfiles=False, allfiles=False, remove=False, reset_all=False, verbose=False)
        
        # Check first call arg
        args, _ = mock_log_error.call_args_list[0]
        assert "not found" in args[0]

def test_rmfile_valid_file_execution(mock_dependencies):
    """Test successful removal of a file."""
    deps = mock_dependencies
    # Mock existing files
    # The code does: valid_files_result = repo.client.query("SELECT DISTINCT source_file ...")
    deps["ch"].client.query.return_value.result_rows = [("target.txt",)]
    
    deps["confirm"].ask.return_value = True
    
    with patch("leakharvester.cli.commands.db.log_success") as mock_log_success:
        db_command(rmfile="target.txt", path=None, init=False, compression=3, status=False, lsfiles=False, allfiles=False, remove=False, reset_all=False, verbose=False)
        
        # Verify ALTER TABLE DELETE
        # The query is: "ALTER TABLE vault.breach_records DELETE WHERE source_file IN ('target.txt')"
        # Verify client.command was called with this.
        
        calls = deps["ch"].client.command.mock_calls
        assert any("DELETE WHERE source_file IN ('target.txt')" in str(c) for c in calls)
        assert any("OPTIMIZE TABLE" in str(c) for c in calls)
        
        mock_log_success.assert_called()

def test_rmfile_user_abort(mock_dependencies):
    """Test user saying 'no' to confirmation."""
    deps = mock_dependencies
    deps["ch"].client.query.return_value.result_rows = [("target.txt",)]
    deps["confirm"].ask.return_value = False
    
    db_command(rmfile="target.txt", path=None, init=False, compression=3, status=False, lsfiles=False, allfiles=False, remove=False, reset_all=False, verbose=False)
    
    # Verify NO delete command
    assert not any("DELETE" in str(c) for c in deps["ch"].client.command.mock_calls)

