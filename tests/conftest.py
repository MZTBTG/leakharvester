import pytest
from pathlib import Path
import shutil

@pytest.fixture
def temp_dirs(tmp_path):
    raw = tmp_path / "raw"
    staging = tmp_path / "staging"
    quarantine = tmp_path / "quarantine"
    raw.mkdir()
    staging.mkdir()
    quarantine.mkdir()
    return raw, staging, quarantine
