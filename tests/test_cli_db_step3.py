import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path
from leakharvester.cli.commands.db import db_command
import typer

@pytest.fixture
def mock_dependencies():
    with patch("leakharvester.cli.commands.db.SettingsManager") as sm_mock, \
         patch("leakharvester.cli.commands.db.ClickHouseAdapter") as ch_mock, \
         patch("leakharvester.cli.commands.db.Console") as console_mock, \
         patch("leakharvester.cli.commands.db.Confirm") as confirm_mock, \
         patch("leakharvester.cli.commands.db.ensure_db_running") as ensure_mock:
        
        sm_instance = sm_mock.return_value
        sm_instance.get_active_db_path.return_value = Path("/tmp/active_db")
        
        ch_instance = ch_mock.return_value
        
        yield {
            "sm": sm_instance,
            "ch": ch_instance,
            "console": console_mock.return_value,
            "confirm": confirm_mock,
            "ensure_db": ensure_mock
        }

def test_db_command_path_update(mock_dependencies):
    """Test updating the active DB path."""
    deps = mock_dependencies
    new_path = Path("/tmp/new_db")
    
    # Case 1: Path exists, is dir, user confirms
    new_path_mock = MagicMock(spec=Path)
    new_path_mock.exists.return_value = True
    new_path_mock.is_dir.return_value = True
    new_path_mock.iterdir.return_value = [] # empty
    new_path_mock.resolve.return_value = "/tmp/new_db"
    
    deps["confirm"].ask.return_value = True
    
    db_command(path=new_path_mock)
    
    deps["sm"].set_active_db_path.assert_called_with(new_path_mock)

def test_db_command_path_create(mock_dependencies):
    """Test creating a new DB path."""
    deps = mock_dependencies
    new_path_mock = MagicMock(spec=Path)
    new_path_mock.exists.return_value = False
    new_path_mock.resolve.return_value = "/tmp/new_db"
    
    deps["confirm"].ask.return_value = True
    
    db_command(path=new_path_mock)
    
    new_path_mock.mkdir.assert_called_with(parents=True, exist_ok=True)
    deps["sm"].set_active_db_path.assert_called_with(new_path_mock)

def test_db_command_status(mock_dependencies):
    """Test --status flag."""
    deps = mock_dependencies
    deps["ch"].get_table_stats.return_value = {"total_rows": 100}
    
    db_command(path=None, status=True, init=False, lsfiles=False, rmfile=None, allfiles=False, remove=False, reset_all=False, verbose=False)
    
    deps["ensure_db"].assert_called_once()
    deps["ch"].get_table_stats.assert_called_with("vault.breach_records")
    assert any("Database is Online" in str(c) for c in deps["console"].print.mock_calls)
    assert any("Records: 100" in str(c) for c in deps["console"].print.mock_calls)

def test_db_command_lsfiles_empty(mock_dependencies):
    """Test --lsfiles with no data."""
    deps = mock_dependencies
    deps["ch"].client.query.return_value.result_rows = []
    
    db_command(path=None, status=False, init=False, lsfiles=True, rmfile=None, allfiles=False, remove=False, reset_all=False, verbose=False)
    
    deps["ensure_db"].assert_called_once()
    assert any("No files found" in str(c) for c in deps["console"].print.mock_calls)

def test_db_command_lsfiles_populated(mock_dependencies):
    """Test --lsfiles with data."""
    deps = mock_dependencies
    deps["ch"].client.query.return_value.result_rows = [
        ("file1.txt", 50, "2023-01-01", "2023-01-02"),
        ("file2.txt", 100, "2023-01-03", "2023-01-04")
    ]
    
    db_command(path=None, status=False, init=False, lsfiles=True, rmfile=None, allfiles=False, remove=False, reset_all=False, verbose=False)
    
    # Check that a Table was printed
    assert deps["console"].print.called

def test_db_command_path_invalid(mock_dependencies):
    """Test providing a file path instead of directory."""
    deps = mock_dependencies
    path_mock = MagicMock(spec=Path)
    path_mock.exists.return_value = True
    path_mock.is_dir.return_value = False
    
    with pytest.raises(typer.Exit) as exc:
        db_command(path=path_mock, status=False, init=False, lsfiles=False, rmfile=None, allfiles=False, remove=False, reset_all=False, verbose=False)
    
    assert exc.value.exit_code == 1
