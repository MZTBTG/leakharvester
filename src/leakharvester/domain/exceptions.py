class LeakHarvesterError(Exception):
    """Base exception for LeakHarvester."""
    pass

class DataValidationError(LeakHarvesterError):
    """Raised when data fails validation rules."""
    pass

class IngestionError(LeakHarvesterError):
    """Raised when an error occurs during the ingestion process."""
    pass

class StorageError(LeakHarvesterError):
    """Raised when an error occurs during storage operations."""
    pass
