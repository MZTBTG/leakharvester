import pytest
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner
from leakharvester.cli.main import app

runner = CliRunner()

@pytest.fixture
def mock_settings_manager(tmp_path):
    with patch("leakharvester.cli.commands.db.SettingsManager") as MockSM:
        sm_instance = MockSM.return_value
        sm_instance.get_active_db_path.return_value = tmp_path / "new_db_path"
        yield sm_instance

@pytest.fixture
def mock_subprocess():
    with patch("subprocess.run") as mock_run:
        yield mock_run

@pytest.fixture
def mock_is_online():
    # Mock is_online inside ensure_db_running. 
    # Since ensure_db_running is imported in db.py, we might need to patch where it's defined or used.
    # Actually ensure_db_running is defined in db.py.
    # We can patch urllib.request.urlopen to simulate online/offline.
    pass

def test_db_init_restarts_docker_container(mock_settings_manager, mock_subprocess):
    """
    Verifies that 'db --init' forces a Docker container restart to apply volume changes,
    even if the database appears online.
    """
    
    # Mock urllib to say DB is ONLINE
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = b"Ok."
        mock_urlopen.return_value.__enter__.return_value = mock_response
        
        # Mock ClickHouseAdapter to avoid actual connection
        with patch("leakharvester.cli.commands.db.ClickHouseAdapter"):
            result = runner.invoke(app, ["db", "--init"])
            
            assert result.exit_code == 0
            
            # CRITICAL ASSERTION (Red Phase):
            # We expect 'docker compose down' to be called to stop the old container
            # And 'docker compose up' with '--force-recreate' (or similar) to start the new one.
            
            # Flatten the calls to check for "down" and "up"
            calls = [str(call.args[0]) for call in mock_subprocess.call_args_list if call.args]
            
            # Check for 'down'
            down_called = any("['docker', 'compose', 'down']" in str(c) for c in calls)
            # Check for 'up' with recreate
            up_called = any("['docker', 'compose', 'up', '-d', '--force-recreate', 'clickhouse']" in str(c) for c in calls)
            
            assert down_called, "docker compose down was NOT called during init"
            assert up_called, "docker compose up --force-recreate was NOT called during init"

