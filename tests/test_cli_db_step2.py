import pytest
from unittest.mock import MagicMock, patch, mock_open, call
import subprocess
import urllib.request
import typer
from leakharvester.cli.commands.db import ensure_db_running
from leakharvester.config import settings

from pathlib import Path

@pytest.fixture
def mock_settings_manager():
    with patch("leakharvester.cli.commands.db.SettingsManager") as mock:
        instance = mock.return_value
        instance.get_active_db_path.return_value = Path("/tmp/db")
        instance.get_container_runtime_root.return_value = Path("/tmp/runtime")
        instance.get_docker_compose_path.return_value = Path("/tmp/docker-compose.yml")
        yield instance

@pytest.fixture
def mock_urlopen():
    with patch("urllib.request.urlopen") as mock:
        yield mock

@pytest.fixture
def mock_subprocess_run():
    with patch("subprocess.run") as mock:
        yield mock

@pytest.fixture
def mock_shutil_which():
    with patch("shutil.which") as mock:
        mock.return_value = "/usr/bin/docker"
        yield mock

@pytest.fixture
def mock_path_exists():
    with patch("pathlib.Path.exists") as mock:
        mock.return_value = True
        yield mock

@pytest.fixture
def mock_time_sleep():
    with patch("time.sleep") as mock:
        yield mock

def test_ensure_db_running_online(mock_urlopen, mock_subprocess_run):
    """Case A: DB is already online."""
    mock_urlopen.return_value.__enter__.return_value.read.return_value = b"Ok."
    
    ensure_db_running()
    
    mock_urlopen.assert_called()
    mock_subprocess_run.assert_not_called()

def test_ensure_db_running_offline_then_success(mock_urlopen, mock_subprocess_run, mock_settings_manager, mock_shutil_which, mock_path_exists, mock_time_sleep):
    """Case B: DB is offline, starts successfully."""
    # First call fails (offline), subsequent calls succeed (online after start)
    mock_urlopen.side_effect = [urllib.error.URLError("Refused"), MagicMock(__enter__=MagicMock(return_value=MagicMock(read=MagicMock(return_value=b"Ok."))))]
    
    ensure_db_running()
    
    # Verify docker compose up was called
    # We expect 'docker compose version' check first (due to get_docker_cmd), then 'up'
    assert mock_subprocess_run.call_count >= 2
    args, _ = mock_subprocess_run.call_args
    assert "up" in args[0]
    assert "clickhouse" in args[0]

def test_ensure_db_running_start_failure(mock_urlopen, mock_subprocess_run, mock_settings_manager, mock_shutil_which, mock_path_exists):
    """Case C: Docker start fails."""
    mock_urlopen.side_effect = urllib.error.URLError("Refused")
    # Mock 'docker compose version' success, but 'up' failure
    mock_subprocess_run.side_effect = [
        MagicMock(returncode=0), # version check
        subprocess.CalledProcessError(1, ["docker", "compose", "up"], stderr="Generic Error") # up command
    ]
    
    with pytest.raises(typer.Exit) as exc:
        ensure_db_running()
    assert exc.value.exit_code == 1

    def test_ensure_db_running_permission_error(mock_urlopen, mock_subprocess_run, mock_settings_manager, mock_shutil_which, mock_path_exists):
        """Case D: Docker permission denied."""
        mock_urlopen.side_effect = urllib.error.URLError("Refused")
        mock_subprocess_run.side_effect = [
            MagicMock(returncode=0),
            subprocess.CalledProcessError(1, ["docker", "compose", "up"], stderr="permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock")
        ]
    
        with patch("leakharvester.cli.commands.db.Console.print") as mock_console:
            with pytest.raises(typer.Exit) as exc:
                ensure_db_running()
            
            assert exc.value.exit_code == 1
            # Check if the helpful message was printed
            assert any("usermod -aG docker" in str(call) for call in mock_console.call_args_list)
def test_ensure_db_running_timeout(mock_urlopen, mock_subprocess_run, mock_settings_manager, mock_shutil_which, mock_path_exists, mock_time_sleep):
    """Test timeout loop if DB never comes online."""
    mock_urlopen.side_effect = urllib.error.URLError("Refused")
    mock_subprocess_run.return_value.returncode = 0 
    
    # We want to break the loop or mock it to be short, but the code has a fixed loop.
    # We can mock range to be short
    with patch("builtins.range", return_value=[0]): 
        with pytest.raises(typer.Exit) as exc:
            ensure_db_running()
        assert exc.value.exit_code == 1

