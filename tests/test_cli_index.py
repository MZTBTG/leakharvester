from typer.testing import CliRunner
from leakharvester.cli.main import app
from unittest.mock import patch, MagicMock

runner = CliRunner()

@patch("leakharvester.cli.commands.index.ClickHouseAdapter")
@patch("leakharvester.cli.commands.index.IndexManager")
def test_index_list(mock_manager_cls, mock_repo_cls):
    """Test 'index --list'."""
    mock_manager = mock_manager_cls.return_value
    mock_manager.list_indexes.return_value = [
        {"name": "idx_email", "column": "email", "type": "inverted", "granularity": 1, "size": "10MB"}
    ]
    
    result = runner.invoke(app, ["index", "--list"])
    
    assert result.exit_code == 0
    assert "Active Indexes" in result.output
    assert "idx_email" in result.output
    assert "inverted" in result.output

@patch("leakharvester.cli.commands.index.ClickHouseAdapter")
@patch("leakharvester.cli.commands.index.IndexManager")
@patch("leakharvester.cli.commands.index.HeuristicAnalyzer")
@patch("leakharvester.cli.commands.index.Prompt")
def test_index_auto_optimize(mock_prompt, mock_analyzer_cls, mock_manager_cls, mock_repo_cls):
    """Test 'index --auto-optimize'."""
    # Mock analyzer response
    mock_rec = MagicMock()
    mock_rec.type = "tokenbf_v1"
    mock_rec.confidence = 0.9
    mock_rec.reason = "Good fit"
    mock_rec.ddl_params = "TYPE tokenbf_v1..."
    
    mock_analyzer = mock_analyzer_cls.return_value
    mock_analyzer.analyze_column.return_value = mock_rec
    
    # Mock Repo to return columns
    mock_repo_instance = mock_repo_cls.return_value
    mock_repo_instance.get_columns.return_value = ["email"]
    
    # Mock User Input (Accept)
    mock_prompt.ask.return_value = "a"
    
    result = runner.invoke(app, ["index", "--auto-optimize"])
    
    assert result.exit_code == 0
    mock_analyzer.analyze_column.assert_called()
    mock_manager_cls.return_value.apply_index.assert_called_with("email", "TYPE tokenbf_v1...")