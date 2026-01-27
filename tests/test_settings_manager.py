import json
import os
from pathlib import Path
from unittest.mock import patch
from leakharvester.settings_manager import SettingsManager

def test_settings_manager_init_creates_missing_file(tmp_path):
    """
    Verifies that initializing SettingsManager when no settings files exist
    results in the creation of a default settings file at the home config location.
    """
    fake_home = tmp_path / "fake_home"
    fake_local_cwd = tmp_path / "fake_cwd"
    fake_local_cwd.mkdir()
    
    home_config_path = fake_home / ".config" / "leakharvester" / "lh-settings.json"
    
    # Ensure environment is clean
    if home_config_path.exists():
        home_config_path.unlink()

    original_cwd = os.getcwd()
    os.chdir(fake_local_cwd)
    
    try:
        with patch("pathlib.Path.home", return_value=fake_home):
             manager = SettingsManager()
             
             # This assertion is expected to FAIL in the Red phase
             # because the current implementation does NOT create the file.
             assert home_config_path.exists(), "Settings file should be created if missing"
             
             content = json.loads(home_config_path.read_text())
             assert content == {}, "Created settings file should contain default empty JSON"
    finally:
        os.chdir(original_cwd)