import pytest
from typer.testing import CliRunner
from unittest.mock import patch, MagicMock
from leakharvester.main import app

runner = CliRunner()

class MockClickHouseClient:
    def __init__(self):
        self.commands = []
        self.queries = []
        self.last_settings = {}
    
    def command(self, cmd, settings=None):
        self.commands.append(cmd)
        if settings:
            self.last_settings.update(settings)
    
    def query(self, sql):
        self.queries.append(sql)
        mock_result = MagicMock()
        mock_result.result_rows = [[None]]
        return mock_result

@pytest.fixture
def mock_adapter():
    with patch("leakharvester.main.ClickHouseAdapter") as MockAdapter:
        mock_instance = MockAdapter.return_value
        mock_client = MockClickHouseClient()
        mock_instance.client = mock_client
        yield mock_client

def test_switch_mode_inverted(mock_adapter):
    """Test that switch-mode --inverted-index generates correct DDL."""
    result = runner.invoke(app, ["switch-mode", "turbo", "--inverted-index"])
    
    assert result.exit_code == 0
    assert "Switching to Turbo Mode" in result.stdout
    assert "Inverted Index (Full Text)" in result.stdout
    
    commands = mock_adapter.commands
    
    # 1. Drop Index
    assert any("DROP INDEX IF EXISTS idx_email" in cmd for cmd in commands)
    
    # 2. Add Index with inverted(0)
    expected_add_index = "ALTER TABLE vault.breach_records ADD INDEX idx_email email TYPE inverted(0) GRANULARITY 1"
    assert any(expected_add_index in cmd for cmd in commands)
    
    # 3. Verify Settings
    assert mock_adapter.last_settings.get("allow_experimental_inverted_index") == 1
