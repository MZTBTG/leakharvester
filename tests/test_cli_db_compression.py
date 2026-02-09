from typer.testing import CliRunner
from leakharvester.cli.main import app
from unittest.mock import patch, MagicMock

runner = CliRunner()

@patch("leakharvester.cli.commands.db.SettingsManager")
@patch("leakharvester.cli.commands.db.ensure_db_running")
@patch("leakharvester.cli.commands.db.ClickHouseAdapter")
def test_db_init_compression_custom(mock_repo_cls: MagicMock, mock_ensure: MagicMock, mock_sm_cls: MagicMock) -> None:
    """Test 'db --init --compression 8'."""
    mock_sm_instance = mock_sm_cls.return_value
    mock_repo = mock_repo_cls.return_value

    result = runner.invoke(app, ["db", "--init", "--compression", "8"])

    assert result.exit_code == 0
    
    # Verify SettingsManager saved the compression level
    mock_sm_instance.set_compression_level.assert_called_with(8)
    
    # Verify execute_ddl was called with SQL containing ZSTD(8)
    calls = mock_repo.execute_ddl.call_args_list
    found_zstd_8 = False
    for call in calls:
        sql = call[0][0]
        if "CODEC(ZSTD(8))" in sql:
            found_zstd_8 = True
            break
    assert found_zstd_8, "SQL with ZSTD(8) was not executed"

@patch("leakharvester.cli.commands.db.SettingsManager")
@patch("leakharvester.cli.commands.db.ensure_db_running")
@patch("leakharvester.cli.commands.db.ClickHouseAdapter")
def test_db_init_compression_default(mock_repo_cls: MagicMock, mock_ensure: MagicMock, mock_sm_cls: MagicMock) -> None:
    """Test 'db --init' defaults to ZSTD(3)."""
    mock_sm_instance = mock_sm_cls.return_value
    mock_repo = mock_repo_cls.return_value

    result = runner.invoke(app, ["db", "--init"])

    assert result.exit_code == 0
    
    # Verify SettingsManager saved the default compression level (3)
    mock_sm_instance.set_compression_level.assert_called_with(3)
    
    # Verify execute_ddl was called with SQL containing ZSTD(3)
    calls = mock_repo.execute_ddl.call_args_list
    found_zstd_3 = False
    for call in calls:
        sql = call[0][0]
        if "CODEC(ZSTD(3))" in sql:
            found_zstd_3 = True
            break
    assert found_zstd_3, "SQL with ZSTD(3) was not executed"

@patch("leakharvester.cli.commands.db.SettingsManager")
@patch("leakharvester.cli.commands.db.ensure_db_running")
@patch("leakharvester.cli.commands.db.ClickHouseAdapter")
def test_db_init_compression_invalid(mock_repo_cls: MagicMock, mock_ensure: MagicMock, mock_sm_cls: MagicMock) -> None:
    """Test 'db --init --compression 25' fails."""
    result = runner.invoke(app, ["db", "--init", "--compression", "25"])
    
    assert result.exit_code == 1
    assert "Compression level must be between 1 and 19" in result.stdout