import json
from pathlib import Path
from typing import Optional, Dict, Any
from leakharvester.adapters.console import log_info, log_error

class SettingsManager:
    """
    Manages persistent settings for LeakHarvester CLI.
    Hierarchy:
    1. User Home: ~/.config/leakharvester/lh-settings.json
    2. Project Root: ./lh-settings.json
    """
    
    def __init__(self):
        self.home_config = Path.home() / ".config" / "leakharvester" / "lh-settings.json"
        self.local_config = Path("lh-settings.json")
        self._settings: Dict[str, Any] = self._load_settings()

    def _load_settings(self) -> Dict[str, Any]:
        """Loads settings from the highest priority existing file."""
        if self.home_config.exists():
            return self._read_json(self.home_config)
        
        if self.local_config.exists():
            return self._read_json(self.local_config)
            
        # If no config exists, create a default one in home_config
        log_info(f"No configuration found. Creating default settings at {self.home_config}")
        default_settings: Dict[str, Any] = {}
        self.save_settings(default_settings, local=False)
        return default_settings

    def _read_json(self, path: Path) -> Dict[str, Any]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            log_error(f"Failed to read settings from {path}: {e}")
            return {}

    def save_settings(self, settings: Dict[str, Any], local: bool = False) -> None:
        """Saves settings to the appropriate file."""
        target = self.local_config if local else self.home_config
        
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps(settings, indent=2), encoding="utf-8")
            self._settings = settings
        except Exception as e:
            log_error(f"Failed to save settings to {target}: {e}")

    def get_active_db_path(self) -> Optional[Path]:
        path_str = self._settings.get("active_db_path")
        return Path(path_str) if path_str else None

    def set_active_db_path(self, path: Path, local: bool = False) -> None:
        self._settings["active_db_path"] = str(path.resolve())
        self.save_settings(self._settings, local=local)

    def get_instance_id(self) -> Optional[str]:
        return self._settings.get("instance_id")

    def set_instance_id(self, instance_id: str, local: bool = False) -> None:
        self._settings["instance_id"] = instance_id
        self.save_settings(self._settings, local=local)

    def get_all(self) -> Dict[str, Any]:
        return self._settings
