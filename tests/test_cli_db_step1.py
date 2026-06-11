import pytest
from unittest.mock import MagicMock, patch, mock_open
import subprocess
import os
from pathlib import Path
from leakharvester.cli.commands.db import get_docker_cmd, get_clickhouse_env, get_ddl_sql
from leakharvester.settings_manager import SettingsManager

@pytest.fixture
def mock_settings_manager():
    with patch("leakharvester.cli.commands.db.SettingsManager") as mock:
        instance = mock.return_value
        instance.get_active_db_path.return_value = Path("/tmp/db")
        instance.get_container_runtime_root.return_value = Path("/tmp/runtime")
        instance.get_docker_compose_path.return_value = Path("/tmp/docker-compose.yml")
        yield instance

@pytest.fixture
def mock_shutil_which():
    with patch("shutil.which") as mock:
        yield mock

@pytest.fixture
def mock_subprocess_run():
    with patch("subprocess.run") as mock:
        yield mock

def test_get_docker_cmd_docker_compose_plugin(mock_shutil_which, mock_subprocess_run):
    """Test detection of 'docker compose' (plugin)."""
    mock_shutil_which.side_effect = lambda cmd: "/usr/bin/docker" if cmd == "docker" else None
    mock_subprocess_run.return_value.returncode = 0  # docker compose version succeeds
    
    cmd = get_docker_cmd()
    assert cmd == ["docker", "compose"]
    
    cmd_with_file = get_docker_cmd("file.yml")
    assert cmd_with_file == ["docker", "compose", "-f", "file.yml"]

def test_get_docker_cmd_standalone(mock_shutil_which, mock_subprocess_run):
    """Test detection of 'docker-compose' (standalone) when plugin is missing/fails."""
    # Case 1: docker exists but 'docker compose version' fails
    mock_shutil_which.side_effect = lambda cmd: "/usr/bin/docker" if cmd == "docker" or cmd == "docker-compose" else None
    mock_subprocess_run.side_effect = subprocess.CalledProcessError(1, ["docker", "compose"])
    
    cmd = get_docker_cmd()
    assert cmd == ["docker-compose"]

    # Case 2: docker does not exist, only docker-compose
    mock_subprocess_run.side_effect = None # reset
    mock_shutil_which.side_effect = lambda cmd: "/usr/bin/docker-compose" if cmd == "docker-compose" else None
    
    cmd = get_docker_cmd()
    assert cmd == ["docker-compose"]

def test_get_docker_cmd_none(mock_shutil_which, mock_subprocess_run):
    """Test when no docker command is found."""
    mock_shutil_which.return_value = None
    assert get_docker_cmd() is None

def test_get_clickhouse_env(mock_settings_manager):
    """Test environment variable generation."""
    env = get_clickhouse_env()
    assert env["LOG_PATH"] == "/tmp/runtime/clickhouse_logs"
    assert env["CONFIG_PATH"] == "/tmp/runtime/clickhouse_config"
    assert env["DB_VOLUME_PATH"] == "/tmp/db"

    # Test when active_path is None
    mock_settings_manager.get_active_db_path.return_value = None
    env = get_clickhouse_env()
    assert env["DB_VOLUME_PATH"] == "/tmp/runtime/clickhouse_data"

def test_get_ddl_sql():
    """Test SQL generation with compression level."""
    sql = get_ddl_sql(5)
    assert "ZSTD(5)" in sql
    assert "CREATE DATABASE IF NOT EXISTS vault" in sql
    assert "CREATE TABLE IF NOT EXISTS vault.breach_records" in sql
