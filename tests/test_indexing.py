import pytest
from typer.testing import CliRunner
from unittest.mock import patch, MagicMock
from leakharvester.main import app
from leakharvester.adapters.clickhouse import ClickHouseAdapter

runner = CliRunner()

class MockClickHouseClient:
    def __init__(self):
        self.commands = []
        self.queries = []
    
    def command(self, cmd, settings=None):
        self.commands.append(cmd)
    
    def query(self, sql):
        self.queries.append(sql)
        # Mocking mutation status to be "done" immediately for loop exit
        mock_result = MagicMock()
        mock_result.result_rows = [[None]] # No active mutations
        return mock_result

@pytest.fixture
def mock_adapter():
    with patch("leakharvester.main.ClickHouseAdapter") as MockAdapter:
        mock_instance = MockAdapter.return_value
        mock_client = MockClickHouseClient()
        mock_instance.client = mock_client
        yield mock_client

def test_switch_mode_token_type(mock_adapter):
    """Test that switch-mode --token-type token generates correct DDL."""
    result = runner.invoke(app, ["switch-mode", "eco", "--token-type", "token"])
    
    assert result.exit_code == 0
    assert "Switching to Eco Mode" in result.stdout
    assert "TokenBF Index" in result.stdout
    
    # Verify DDL commands
    commands = mock_adapter.commands
    
    # 1. Drop Index
    assert any("DROP INDEX IF EXISTS idx_email" in cmd for cmd in commands)
    
    # 2. Add Index with tokenbf_v1 on lower(email)
    expected_add_index = "ALTER TABLE vault.breach_records ADD INDEX idx_email lower(email) TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1"
    assert any(expected_add_index in cmd for cmd in commands)
    
    # 3. Materialize
    assert any("MATERIALIZE INDEX idx_email" in cmd for cmd in commands)

def test_switch_mode_default_ngram(mock_adapter):
    """Test that switch-mode (default) uses ngrambf_v1."""
    result = runner.invoke(app, ["switch-mode", "turbo"])
    
    assert result.exit_code == 0
    assert "Trigram Index" in result.stdout
    
    commands = mock_adapter.commands
    
    # Verify Ngram definition
    # Turbo size = 262144
    expected_add_index = "ALTER TABLE vault.breach_records ADD INDEX idx_email email TYPE ngrambf_v1(3, 262144, 2, 0) GRANULARITY 1"
    assert any(expected_add_index in cmd for cmd in commands)
