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

class SchemaMismatchError(IngestionError):
    """Raised when the input schema does not match the database schema."""
    def __init__(self, missing_columns: list[str]):
        self.missing_columns = missing_columns
        super().__init__(f"Missing columns in database: {missing_columns}")
