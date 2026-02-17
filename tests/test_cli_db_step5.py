import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path
from leakharvester.cli.commands.db import db_command
import shutil
import subprocess

@pytest.fixture
def mock_dependencies():
    with (
        patch("leakharvester.cli.commands.db.SettingsManager") as sm_mock,
        patch("leakharvester.cli.commands.db.ClickHouseAdapter") as ch_mock,
        patch("leakharvester.cli.commands.db.Console") as console_mock,
        patch("leakharvester.cli.commands.db.Confirm") as confirm_mock,
        patch("leakharvester.cli.commands.db.Prompt") as prompt_mock,
        patch("leakharvester.cli.commands.db.ensure_db_running") as ensure_mock,
        patch("leakharvester.cli.commands.db.get_docker_cmd") as get_docker_mock,
        patch("shutil.rmtree") as rmtree_mock,
        patch("subprocess.run") as subprocess_mock,
        patch("pathlib.Path.exists") as path_exists_mock
    ):
        
        path_exists_mock.return_value = True
        
        sm_instance = sm_mock.return_value
        sm_instance.get_active_db_path.return_value = Path("/tmp/active_db")
        sm_instance.home_config.exists.return_value = True
        sm_instance.local_config.exists.return_value = True
        sm_instance.get_docker_compose_path.return_value = Path("/tmp/dc.yml")
        
        get_docker_mock.return_value = ["docker", "compose"]
        
        yield {
            "sm": sm_instance,
            "ch": ch_mock.return_value,
            "console": console_mock.return_value,
            "confirm": confirm_mock,
            "prompt": prompt_mock,
            "ensure_db": ensure_mock,
            "rmtree": rmtree_mock,
            "subprocess": subprocess_mock
        }

def test_allfiles_abort(mock_dependencies):
    """Test --allfiles with incorrect confirmation."""
    deps = mock_dependencies
    deps["prompt"].ask.return_value = "wrong"
    
    db_command(allfiles=True, path=None, init=False, compression=3, status=False, lsfiles=False, rmfile=None, remove=False, reset_all=False, verbose=False)
    
    deps["ch"].client.command.assert_not_called()

def test_allfiles_success(mock_dependencies):
    """Test --allfiles with correct confirmation."""
    deps = mock_dependencies
    deps["prompt"].ask.return_value = "wipe"
    
    db_command(allfiles=True, path=None, init=False, compression=3, status=False, lsfiles=False, rmfile=None, remove=False, reset_all=False, verbose=False)
    
    deps["ch"].client.command.assert_called_with("TRUNCATE TABLE vault.breach_records", settings={'max_table_size_to_drop': 0})

def test_remove_abort(mock_dependencies):
    """Test --remove with incorrect confirmation."""
    deps = mock_dependencies
    deps["confirm"].ask.return_value = False
    
    db_command(remove=True, path=None, init=False, compression=3, status=False, lsfiles=False, rmfile=None, allfiles=False, reset_all=False, verbose=False)
    
    deps["rmtree"].assert_not_called()
    deps["subprocess"].assert_not_called()

def test_remove_success(mock_dependencies):
    """Test --remove with success."""
    deps = mock_dependencies
    deps["confirm"].ask.return_value = True
    
    db_command(remove=True, path=None, init=False, compression=3, status=False, lsfiles=False, rmfile=None, allfiles=False, reset_all=False, verbose=False)
    
    # Should stop docker first
    # get_docker_cmd returns ["docker", "compose"], so call is ["docker", "compose", "down"]
    # Verify subprocess calls
    calls = deps["subprocess"].mock_calls
    # call(['docker', 'compose', 'down'], check=False)
    
    has_down = any("down" in str(c) for c in calls)
    assert has_down
    
    # Should delete tree
    deps["rmtree"].assert_called_with(Path("/tmp/active_db"))

def test_reset_all_abort(mock_dependencies):
    """Test --reset-all with incorrect confirmation."""
    deps = mock_dependencies
    deps["confirm"].ask.return_value = True # Initial confirm
    deps["prompt"].ask.return_value = "wrong" # Second safety check
    
    db_command(reset_all=True, path=None, init=False, compression=3, status=False, lsfiles=False, rmfile=None, allfiles=False, remove=False, verbose=False)
    
    deps["rmtree"].assert_not_called()
    deps["subprocess"].assert_not_called()

def test_reset_all_success(mock_dependencies):
    """Test --reset-all full sequence."""
    deps = mock_dependencies
    deps["confirm"].ask.return_value = True
    deps["prompt"].ask.return_value = "reset all"
    
    db_command(reset_all=True, path=None, init=False, compression=3, status=False, lsfiles=False, rmfile=None, allfiles=False, remove=False, verbose=False)
    
    # Check Docker kill
    calls = deps["subprocess"].mock_calls
    
    # We expect kill and down -v
    has_kill = any("kill" in str(c) for c in calls)
    has_down = any("down" in str(c) for c in calls)
    
    # Also pkill check
    has_pkill = any("pkill" in str(c) for c in calls)
    
    assert has_kill
    assert has_down
    assert has_pkill
    
    # Check file removal
    assert deps["sm"].home_config.unlink.called
    assert deps["sm"].local_config.unlink.called
    
    # Data removal
    deps["rmtree"].assert_called_with(Path("/tmp/active_db"))
