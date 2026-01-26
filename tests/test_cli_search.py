from typer.testing import CliRunner
from leakharvester.cli.main import app
from unittest.mock import patch, MagicMock
import pytest

runner = CliRunner()

@patch("leakharvester.cli.commands.search.ClickHouseAdapter")
def test_search_basic(mock_repo_cls):
    """Test 'search query' basic execution."""
    mock_repo = mock_repo_cls.return_value
    mock_repo.get_columns.return_value = ["email", "password"]
    
    # Mock query result
    mock_result = MagicMock()
    mock_result.result_rows = [["test@example.com", "password123"]]
    mock_result.column_names = ["email", "password"]
    mock_repo.client.query.return_value = mock_result
    
    result = runner.invoke(app, ["search", "test"])
    
    assert result.exit_code == 0
    assert "test@example.com" in result.output
    # Verify SQL contained ILIKE
    args, _ = mock_repo.client.query.call_args
    sql = args[0]
    assert "ILIKE '%test%'" in sql

@patch("leakharvester.cli.commands.search.ClickHouseAdapter")
def test_search_options(mock_repo_cls):
    """Test search options like --limit and --exact."""
    mock_repo = mock_repo_cls.return_value
    mock_repo.get_columns.return_value = ["email"]
    mock_repo.client.query.return_value = MagicMock(result_rows=[], column_names=[])
    
    result = runner.invoke(app, ["search", "test", "--limit", "10", "--exact"])
    
    assert result.exit_code == 0
    
    args, _ = mock_repo.client.query.call_args
    sql = args[0]
    assert "lower(email) = lower('test')" in sql
    assert "LIMIT 10" in sql