from typer.testing import CliRunner
from leakharvester.cli.main import app
from unittest.mock import patch

runner = CliRunner()

@patch("leakharvester.cli.commands.search.ClickHouseAdapter")
def test_search_basic(mock_repo_cls):
    """Test 'search query' basic execution."""
    mock_repo = mock_repo_cls.return_value
    mock_repo.get_columns.return_value = ["email", "password"]
    
    # Mock stream result
    mock_repo.stream_query.return_value = iter([["test@example.com", "password123"]])
    
    result = runner.invoke(app, ["search", "test", "--pretty"])
    
    assert result.exit_code == 0
    assert "test@example.com" in result.output
    # Verify SQL contained ILIKE
    args, _ = mock_repo.stream_query.call_args
    sql = args[0]
    assert "ILIKE '%test%'" in sql

@patch("leakharvester.cli.commands.search.ClickHouseAdapter")
def test_search_options(mock_repo_cls):
    """Test search options like --limit and --exact."""
    mock_repo = mock_repo_cls.return_value
    mock_repo.get_columns.return_value = ["email"]
    mock_repo.stream_query.return_value = iter([])
    
    result = runner.invoke(app, ["search", "test", "--limit", "10", "--exact", "--pretty"])
    
    assert result.exit_code == 0
    
    args, _ = mock_repo.stream_query.call_args
    sql = args[0]
    assert "lower(email) = lower('test')" in sql
    assert "LIMIT 10" in sql