from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

class Settings(BaseSettings):
    # App General
    APP_NAME: str = "LeakHarvester"
    DEBUG: bool = False
    
    # Directories
    DATA_DIR: Path = Path("data")
    RAW_DIR: Path = DATA_DIR / "raw"
    STAGING_DIR: Path = DATA_DIR / "staging"
    QUARANTINE_DIR: Path = DATA_DIR / "quarantine"
    
    # ClickHouse
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = "leakharvester"
    CLICKHOUSE_PASSWORD: str = "secret_password"
    CLICKHOUSE_DB: str = "vault"
    
    # Ingestion
    BATCH_SIZE: int = 500_000
    
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    def create_dirs(self) -> None:
        self.RAW_DIR.mkdir(parents=True, exist_ok=True)
        self.STAGING_DIR.mkdir(parents=True, exist_ok=True)
        self.QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

settings = Settings()
