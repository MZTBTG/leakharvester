from typer.testing import CliRunner
from leakharvester.cli.main import app
from unittest.mock import patch

runner = CliRunner()

@patch("leakharvester.cli.commands.db.SettingsManager")
@patch("leakharvester.cli.commands.db.ClickHouseAdapter")
def test_db_lsfiles_command(mock_repo_cls, mock_sm_cls):
    """Test 'db --lsfiles' command."""
    mock_sm_instance = mock_sm_cls.return_value
    mock_sm_instance.get_active_db_path.return_value = "/tmp/test_db"
    
    mock_repo_instance = mock_repo_cls.return_value
    # Mock return for lsfiles query
    # Columns: source_file, row_count, first_import, last_import
    mock_repo_instance.client.query.return_value.result_rows = [
        ("test_file.csv", 1000, "2023-01-01 12:00:00", "2023-01-02 12:00:00")
    ]

    result = runner.invoke(app, ["db", "--lsfiles"])
    
    # Assert failure (Red Phase)
    # The command doesn't exist yet, so it should fail or print help
    # Or if it executes 'db', it won't have --lsfiles argument
    assert result.exit_code == 0
    assert "Source File" in result.output
    assert "test_file.csv" in result.output

@patch("leakharvester.cli.commands.db.SettingsManager")
@patch("leakharvester.cli.commands.db.ClickHouseAdapter")
def test_db_rmfile_command(mock_repo_cls, mock_sm_cls):
    """Test 'db --rmfile' command."""
    mock_sm_instance = mock_sm_cls.return_value
    mock_sm_instance.get_active_db_path.return_value = "/tmp/test_db"
    
    mock_repo_instance = mock_repo_cls.return_value
    
    # Mock check for existing files
    mock_repo_instance.client.query.return_value.result_rows = [("test_file.csv",)]
    
    # Mock confirmation
    with patch("rich.prompt.Confirm.ask", return_value=True):
        result = runner.invoke(app, ["db", "--rmfile", "test_file.csv"])
    
    assert result.exit_code == 0
    assert "Delete mutation submitted" in result.output

@patch("leakharvester.cli.commands.db.SettingsManager")
@patch("leakharvester.cli.commands.db.ClickHouseAdapter")
def test_db_allfiles_command(mock_repo_cls, mock_sm_cls):
    """Test 'db --allfiles' command."""
    mock_sm_instance = mock_sm_cls.return_value
    mock_sm_instance.get_active_db_path.return_value = "/tmp/test_db"
    
    mock_repo_instance = mock_repo_cls.return_value
    
    # Mock prompt input "wipe"
    with patch("rich.prompt.Prompt.ask", return_value="wipe"):
        result = runner.invoke(app, ["db", "--allfiles"])
    
    assert result.exit_code == 0
    assert "Database truncated" in result.output
