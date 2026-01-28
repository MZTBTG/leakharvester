import pytest
from unittest.mock import patch, MagicMock, ANY
from typer.testing import CliRunner
from leakharvester.cli.main import app
from leakharvester.cli.commands import db

runner = CliRunner()

def test_db_init_generates_and_stores_instance_id(tmp_path):
    """
    Verifies that 'db --init' generates an Instance ID, saves it to settings,
    and stores it in the database.
    """
    
    # Mock dependencies
    with patch("leakharvester.cli.commands.db.SettingsManager") as MockSM, \
         patch("leakharvester.cli.commands.db.ClickHouseAdapter") as MockAdapter, \
         patch("leakharvester.cli.commands.db.ensure_db_running"), \
         patch("uuid.uuid4", return_value="test-uuid-1234"), \
         patch("leakharvester.cli.commands.db.settings") as mock_settings:
         
        mock_settings.create_dirs = MagicMock()
        
        sm_instance = MockSM.return_value
        sm_instance.get_active_db_path.return_value = tmp_path / "test_db"
        
        repo_instance = MockAdapter.return_value
        
        result = runner.invoke(app, ["db", "--init"])
        
        # Verify ID saved to settings
        # FAIL expected here
        sm_instance.set_instance_id.assert_called_with("test-uuid-1234")
        
        # Verify DDL executed for system_info table
        create_table_sql_fragment = "CREATE TABLE IF NOT EXISTS vault.system_info"
        create_calls = [call.args[0] for call in repo_instance.execute_ddl.call_args_list if create_table_sql_fragment in call.args[0]]
        assert create_calls, "system_info table was not created"
        
        # Verify INSERT executed
        execute_calls = [call.args[0] for call in repo_instance.client.command.call_args_list] + \
                        [call.args[0] for call in repo_instance.execute_ddl.call_args_list]
        
        print(f"DEBUG CALLS: {execute_calls}") # Debugging

        insert_found = any("INSERT INTO vault.system_info" in str(c) for c in execute_calls)
        assert insert_found, "Instance ID insert statement not found"

