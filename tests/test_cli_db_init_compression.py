import pytest
from unittest.mock import patch, MagicMock, ANY
from typer.testing import CliRunner
from leakharvester.cli.main import app

runner = CliRunner()

@pytest.fixture
def mock_dependencies():
    with patch("leakharvester.cli.commands.db.SettingsManager") as MockSM, \
         patch("leakharvester.cli.commands.db.ClickHouseAdapter") as MockRepo, \
         patch("leakharvester.cli.commands.db.ensure_db_running") as mock_ensure, \
         patch("leakharvester.cli.commands.db.settings") as mock_settings:
        
        # Setup SettingsManager mock instance
        sm_instance = MockSM.return_value
        sm_instance.get_active_db_path.return_value = None
        
        # Setup Repository mock instance
        repo_instance = MockRepo.return_value
        repo_instance.client.insert = MagicMock()
        
        yield {
            "sm_cls": MockSM,
            "sm": sm_instance,
            "repo_cls": MockRepo,
            "repo": repo_instance,
            "ensure": mock_ensure,
            "settings": mock_settings
        }

def test_db_init_compression_validation_low():
    """Test that compression < 1 is rejected."""
    result = runner.invoke(app, ["db", "--init", "--compression", "0"])
    assert result.exit_code == 1
    assert "Compression level must be between 1 and 19" in result.output

def test_db_init_compression_validation_high():
    """Test that compression > 19 is rejected."""
    result = runner.invoke(app, ["db", "--init", "--compression", "20"])
    assert result.exit_code == 1
    assert "Compression level must be between 1 and 19" in result.output

def test_db_init_success_sequence(mock_dependencies):
    """
    Verify the successful initialization sequence:
    1. DB is forced to restart.
    2. Settings dirs are created.
    3. Compression level is set.
    4. DDL is executed.
    5. Instance ID is generated and stored.
    """
    mocks = mock_dependencies
    
    # Run command
    result = runner.invoke(app, ["db", "--init", "--compression", "5"])
    
    assert result.exit_code == 0
    assert "Database initialized" in result.output
    
    # 1. Verify DB Restart
    mocks["ensure"].assert_called_once_with(force_restart=True, verbose=False)
    
    # 2. Verify Dirs Created
    mocks["settings"].create_dirs.assert_called_once()
    
    # 3. Verify Compression Set
    mocks["sm"].set_compression_level.assert_called_once_with(5)
    
    # 4. Verify DDL Execution
    # We expect multiple calls to execute_ddl because get_ddl_sql returns multiple statements
    assert mocks["repo"].execute_ddl.call_count >= 2
    
    # 5. Verify Instance ID Generation and Storage
    # sm.set_instance_id should be called with a UUID string
    mocks["sm"].set_instance_id.assert_called_once()
    args, _ = mocks["sm"].set_instance_id.call_args
    assert isinstance(args[0], str)
    assert len(args[0]) > 0
    
    # Verify insert into system_info
    mocks["repo"].client.insert.assert_called_once()
    call_args = mocks["repo"].client.insert.call_args
    assert call_args[0][0] == "vault.system_info" # Table name
    assert call_args[1]['column_names'] == ['key', 'value'] # Columns

def test_db_init_failure_handling(mock_dependencies):
    """Verify error handling when DDL execution fails."""
    mocks = mock_dependencies
    
    # Simulate DB error
    mocks["repo"].execute_ddl.side_effect = Exception("Critical SQL Error")
    
    result = runner.invoke(app, ["db", "--init"])
    
    assert result.exit_code == 1
    assert "Failed to initialize DB" in result.output
    assert "Critical SQL Error" in result.output
