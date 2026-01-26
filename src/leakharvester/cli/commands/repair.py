from leakharvester.adapters.clickhouse import ClickHouseAdapter
from leakharvester.adapters.local_fs import LocalFileSystemAdapter
from leakharvester.services.ingestor import BreachIngestor
from leakharvester.config import settings

def repair_command():
    """Attempts to repair and ingest data from the quarantine directory."""
    repo = ClickHouseAdapter()
    fs = LocalFileSystemAdapter()
    ingestor = BreachIngestor(repo, fs)
    ingestor.repair_quarantine(settings.QUARANTINE_DIR, settings.STAGING_DIR)
