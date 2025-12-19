import polars as pl

# Schema for raw CSV processing (before strict typing)
# We treat everything as String initially to be resilient against malformed data
RAW_CSV_SCHEMA = {
    "email": pl.String,
    "username": pl.String,
    "password": pl.String,
    "hash": pl.String,
    "salt": pl.String,
    "breach_date": pl.String,
    "source_file": pl.String
}

# Final Schema for Parquet and ClickHouse
# This must match the ClickHouse DDL
CANONICAL_SCHEMA = pl.Schema({
    "source_file": pl.String,
    "breach_date": pl.Date,
    "import_date": pl.Datetime("us"),
    "email": pl.String,
    "username": pl.String,
    "password": pl.String
})

# Columns required for a valid record (at least one of these must exist)
REQUIRED_IDENTITY_COLUMNS = ["email", "username"]
