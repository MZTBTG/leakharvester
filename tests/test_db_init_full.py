import pytest
from unittest.mock import patch, MagicMock, call
from pathlib import Path
from typer.testing import CliRunner
from leakharvester.cli.main import app

runner = CliRunner()

@pytest.fixture
def mock_settings_manager(tmp_path):
    with patch("leakharvester.cli.commands.db.SettingsManager") as MockSM:
        sm_instance = MockSM.return_value
        # Use tmp_path for all directories
        runtime_root = tmp_path / "container_runtime"
        runtime_root.mkdir()
        (runtime_root / "clickhouse_config").mkdir()
        (runtime_root / "clickhouse_logs").mkdir()
        (runtime_root / "clickhouse_data").mkdir()
        
        sm_instance.get_container_runtime_root.return_value = runtime_root
        sm_instance.get_active_db_path.return_value = None # Use default
        compose_file = tmp_path / "docker-compose.yml"
        compose_file.touch()
        sm_instance.get_docker_compose_path.return_value = compose_file
        
        # Ensure these are mocks so we can assert calls
        sm_instance.ensure_docker_compose_exists = MagicMock()
        
        yield sm_instance

@pytest.fixture
def mock_subprocess():
    with patch("subprocess.run") as mock_run:
        yield mock_run

@pytest.fixture
def mock_shutil_which():
    with patch("shutil.which") as mock_which:
        mock_which.return_value = "/usr/bin/docker"
        yield mock_which

def test_db_init_full_flow(mock_settings_manager, mock_subprocess, mock_shutil_which):
    """
    Verifies the complete 'leakharvester db --init' flow, specifically checking:
    1. SettingsManager prepares environment (config deployment).
    2. Environment variables are correctly injected into subprocess calls.
    3. Container is stopped and restarted.
    """
    
    # Mock urllib to simulate DB coming online
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = b"Ok."
        mock_urlopen.return_value.__enter__.return_value = mock_response
        
        # Mock ClickHouseAdapter
        with patch("leakharvester.cli.commands.db.ClickHouseAdapter"):
            result = runner.invoke(app, ["db", "--init", "--verbose"])
            
            assert result.exit_code == 0
            
            # 1. Verify SettingsManager called ensures
            mock_settings_manager.ensure_docker_compose_exists.assert_called_once()
            
            # 2. Analyze subprocess calls
            # We expect calls to 'docker compose down' and 'docker compose up'
            
            up_call = None
            for call_args in mock_subprocess.call_args_list:
                args = call_args.args[0]
                if "up" in args and "clickhouse" in args:
                    up_call = call_args
                    break
            
            assert up_call is not None, "docker compose up was not called"
            
            # 3. Verify Environment Variables
            # The 'env' kwarg should be passed to subprocess.run
            # It must contain absolute paths to runtime directories
            
            env_vars = up_call.kwargs.get('env')
            assert env_vars is not None
            
            runtime_root = str(mock_settings_manager.get_container_runtime_root.return_value)
            
            assert env_vars["LOG_PATH"] == f"{runtime_root}/clickhouse_logs"
            assert env_vars["CONFIG_PATH"] == f"{runtime_root}/clickhouse_config"
            # Default DB path should also be in runtime root
            assert env_vars["DB_VOLUME_PATH"] == f"{runtime_root}/clickhouse_data"

def test_db_init_timeout_dumps_logs(mock_settings_manager, mock_subprocess, mock_shutil_which):
    """
    Verifies that if DB fails to come online, logs are dumped.
    """
    # Mock urllib to ALWAYS FAIL (simulate timeout)
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_urlopen.side_effect = Exception("Connection refused")
        
        # Patch time.sleep to speed up test (avoid 120s wait)
        with patch("time.sleep"):
            result = runner.invoke(app, ["db", "--init"])
            
            # Should fail with exit code 1
            assert result.exit_code == 1
            
            # Verify log dumping logic
            # Should try to read log file via subprocess (tail) OR docker logs
            # Since we didn't create the log file on disk, it should fallback to 'docker logs'
            
            log_dump_call = None
            for call_args in mock_subprocess.call_args_list:
                args = call_args.args[0]
                if "logs" in args and "clickhouse" in args:
                    log_dump_call = call_args
                    break
            
            assert log_dump_call is not None, "Container logs were not dumped upon timeout"
