from unittest.mock import patch, MagicMock
from typer.testing import CliRunner
from leakharvester.cli.main import app

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
        sm_instance.set_instance_id.assert_called_with("test-uuid-1234")
        
        # Verify DDL executed for system_info table
        create_table_sql_fragment = "CREATE TABLE IF NOT EXISTS vault.system_info"
        create_calls = [call.args[0] for call in repo_instance.execute_ddl.call_args_list if create_table_sql_fragment in call.args[0]]
        assert create_calls, "system_info table was not created"
        
        # Verify INSERT executed using client.insert (not command)
        repo_instance.client.insert.assert_called_with(
            "vault.system_info",
            [['instance_id', "test-uuid-1234"]],
            column_names=['key', 'value']
        )

