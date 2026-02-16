from pathlib import Path
from typing import Optional, Dict, Any
import json
import shutil
import importlib.resources as pkg_resources
from leakharvester.adapters.console import log_info, log_error, log_warning

class SettingsManager:
    """
    Manages persistent settings for LeakHarvester CLI.
    Hierarchy:
    1. User Home: ~/.config/leakharvester/lh-settings.json
    2. Project Root: ./lh-settings.json
    """
    
    def __init__(self) -> None:
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

    def get_compression_level(self) -> int:
        return self._settings.get("compression_level", 3)

    def set_compression_level(self, level: int, local: bool = False) -> None:
        self._settings["compression_level"] = level
        self.save_settings(self._settings, local=local)

    def get_all(self) -> Dict[str, Any]:
        return self._settings

    def get_docker_compose_path(self) -> Path:
        """Returns the path to the deployed docker-compose.yml in the user config dir."""
        return self.home_config.parent / "docker-compose.yml"

    def get_container_runtime_root(self) -> Path:
        """Returns the absolute path to the container runtime directory."""
        return (self.home_config.parent / "container_runtime").resolve()

    def ensure_data_dirs(self) -> None:
        """Ensures data directories for Docker bind mounts exist and are writable."""
        base_dir = self.get_container_runtime_root()
        import os
        current_uid = os.getuid()
        
        for subdir in ["clickhouse_logs", "clickhouse_config", "clickhouse_data"]:
            path = base_dir / subdir
            path.mkdir(parents=True, exist_ok=True)
            
            try:
                stat_info = path.stat()
                # Only attempt chmod if we own the file or are root
                if stat_info.st_uid == current_uid or current_uid == 0:
                    path.chmod(0o777)
                else:
                    log_warning(f"Skipping chmod on {path}: Owned by UID {stat_info.st_uid} (Current: {current_uid})")
            except Exception as e:
                log_error(f"Failed to check/set permissions for {path}: {e}")

    def deploy_config_files(self) -> None:
        """Deploys default configuration files to the runtime config directory."""
        config_dir = self.get_container_runtime_root() / "clickhouse_config"
        target_path = config_dir / "network_config.xml"
        
        # Always deploy/update to ensure connectivity settings are correct
        log_info(f"Deploying network_config.xml to {target_path}...")
        try:
            ref = pkg_resources.files('leakharvester') / 'resources' / 'network_config.xml'
            with pkg_resources.as_file(ref) as source_path:
                shutil.copy2(source_path, target_path)
            # Ensure it's readable by container
            target_path.chmod(0o644)
        except Exception as e:
            log_error(f"Failed to deploy network_config.xml: {e}")
            # Fallback
            possible_source = Path(__file__).parent / "resources" / "network_config.xml"
            if possible_source.exists():
                shutil.copy2(possible_source, target_path)
                target_path.chmod(0o644)

    def ensure_docker_compose_exists(self) -> None:
        """
        Ensures docker-compose.yml exists in the configuration directory.
        If not, it copies it from the package resources.
        """
        self.ensure_data_dirs()
        self.deploy_config_files()
        
        target_path = self.get_docker_compose_path()
        # Always overwrite to ensure path variables are up to date
        log_info(f"Deploying/Updating docker-compose.yml to {target_path}...")
        try:
            ref = pkg_resources.files('leakharvester') / 'resources' / 'docker-compose.yml'
            with pkg_resources.as_file(ref) as source_path:
                shutil.copy2(source_path, target_path)
                
            log_info("Successfully deployed docker-compose.yml.")
        except Exception as e:
            log_error(f"Failed to deploy docker-compose.yml: {e}")
            possible_source = Path(__file__).parent / "resources" / "docker-compose.yml"
            if possible_source.exists():
                shutil.copy2(possible_source, target_path)
                log_info("Deployed via fallback path.")
            else:
                log_error(f"Could not find source docker-compose.yml at {possible_source}")
