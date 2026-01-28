import polars as pl

RAW_CSV_SCHEMA = {
    "email": pl.String,
    "username": pl.String,
    "password": pl.String,
    "hash": pl.String,
    "salt": pl.String,
    "breach_date": pl.String,
    "source_file": pl.String
}

CANONICAL_SCHEMA = pl.Schema({
    "source_file": pl.String,
    "breach_date": pl.Date,
    "import_date": pl.Datetime("us"),
    "email": pl.String,
    "username": pl.String,
    "password": pl.String
})

REQUIRED_IDENTITY_COLUMNS = ["email", "username"]
