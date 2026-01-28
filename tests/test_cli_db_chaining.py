import pytest
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner
from leakharvester.cli.main import app
from leakharvester.cli.commands import db

runner = CliRunner()

def test_db_path_and_init_chaining(tmp_path):
    """
    Verifies that providing both --path and --init executes BOTH logic blocks.
    """
    
    # Mock dependencies
    with patch("leakharvester.cli.commands.db.SettingsManager") as MockSM, \
         patch("leakharvester.cli.commands.db.ClickHouseAdapter") as MockAdapter, \
         patch("leakharvester.cli.commands.db.ensure_db_running"), \
         patch("uuid.uuid4", return_value="test-uuid-chain"), \
         patch("leakharvester.cli.commands.db.settings") as mock_settings, \
         patch("leakharvester.cli.commands.db.Confirm.ask", return_value=True):
         
        mock_settings.create_dirs = MagicMock()
        
        sm_instance = MockSM.return_value
        sm_instance.get_active_db_path.return_value = tmp_path / "chained_db"
        
        repo_instance = MockAdapter.return_value
        
        # We need to verify that init logic is reached.
        # Init logic calls repo.execute_ddl or creates system_info
        
        result = runner.invoke(app, ["db", "--path", str(tmp_path / "chained_db"), "--init"])
        
        assert result.exit_code == 0
        
        # Verify Path was set
        sm_instance.set_active_db_path.assert_called()
        
        # Verify Init was called (check for Instance ID generation/setting)
        # In the current buggy implementation, this assertion should FAIL
        sm_instance.set_instance_id.assert_called_with("test-uuid-chain")

