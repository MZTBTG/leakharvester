import pytest
import importlib

def test_cli_module_exists():
    """Verify that the new CLI module structure exists."""
    try:
        importlib.import_module("leakharvester.cli")
    except ImportError:
        pytest.fail("Could not import 'leakharvester.cli'")

def test_cli_commands_module_exists():
    """Verify that the commands submodule exists."""
    try:
        importlib.import_module("leakharvester.cli.commands")
    except ImportError:
        pytest.fail("Could not import 'leakharvester.cli.commands'")

def test_cli_main_module_exists():
    """Verify that the main CLI entry point exists."""
    try:
        importlib.import_module("leakharvester.cli.main")
    except ImportError:
        pytest.fail("Could not import 'leakharvester.cli.main'")
