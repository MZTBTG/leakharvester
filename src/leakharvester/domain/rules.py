import polars as pl
from typing import List, Dict, Optional

def normalize_text_expr(col_name: str) -> pl.Expr:
    """
    Returns a Polars expression to normalize text columns:
    - Strip whitespace
    - Convert to lowercase
    - Fill nulls with empty string
    """
    return pl.col(col_name).str.strip_chars().str.to_lowercase().fill_null("")

def build_search_blob_expr(columns: List[str]) -> pl.Expr:
    """
    Constructs the _search_blob column by concatenating normalized versions
    of the specified columns.
    """
    normalized_cols = [normalize_text_expr(c) for c in columns]
    return pl.concat_str(normalized_cols, separator=" ").alias("_search_blob")

def detect_column_mapping(columns: List[str]) -> Dict[str, str]:
    """
    Heuristic to map dirty CSV column names to canonical schema names.
    """
    mapping = {}
    for col in columns:
        c_lower = col.lower()
        if "mail" in c_lower:
            mapping[col] = "email"
        elif any(x in c_lower for x in ["user", "login", "usuario", "username"]):
            mapping[col] = "username"
        elif any(x in c_lower for x in ["pass", "senh", "pwd", "password"]):
            mapping[col] = "password"
        elif "hash" in c_lower:
            mapping[col] = "hash"
        elif "salt" in c_lower:
            mapping[col] = "salt"
        elif "date" in c_lower or "data" in c_lower:
             # Very naive date detection, might need refinement
             pass 
    
    return mapping
