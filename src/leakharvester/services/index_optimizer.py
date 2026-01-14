from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from rich.console import Console
from rich.table import Table
from leakharvester.adapters.clickhouse import ClickHouseAdapter
from leakharvester.adapters.console import log_info, log_error, log_warning, log_success

@dataclass
class IndexRecommendation:
    type: str
    confidence: float
    reason: str
    ddl_params: str

class HeuristicAnalyzer:
    def __init__(self, repo: ClickHouseAdapter):
        self.repo = repo
        self.console = Console()

    def analyze_column(self, table: str, column: str, sample_size: int = 10000, random_sample: bool = False) -> IndexRecommendation:
        """
        Analyzes a column's statistical properties to recommend an index strategy.
        """
        # 1. Fetch Stats
        try:
            # Check basic type first
            cols = self.repo.get_columns_with_types(table)
            col_type = next((t for c, t in cols if c == column), None)
            
            if not col_type:
                return IndexRecommendation("NONE", 0.0, "Column not found", "")
            
            if "String" not in col_type and "FixedString" not in col_type:
                return IndexRecommendation("NONE", 1.0, f"Type {col_type} usually needs no text index", "")

            # 2. Heuristic Analysis via SQL (Faster than fetching rows)
            # We calculate entropy proxies: uniq (cardinality), avg_len
            
            sample_clause = f"LIMIT {sample_size}"
            if random_sample:
                sample_clause = f"ORDER BY rand() {sample_clause}"

            sql = f"""
            SELECT 
                uniqExact({column}) as cardinality,
                avg(length({column})) as avg_len,
                count() as total_rows,
                sum(countMatches({column}, '[^a-zA-Z0-9]')) as non_alnum_chars
            FROM (SELECT {column} FROM {table} {sample_clause})
            """
            
            res = self.repo.client.query(sql).result_rows[0]
            cardinality = res[0]
            avg_len = res[1]
            total_rows = res[2]
            avg_non_alnum = res[3] / total_rows if total_rows > 0 else 0

            # 3. Decision Matrix
            
            # Ratio of unique values to total sample
            uniqueness_ratio = cardinality / total_rows if total_rows > 0 else 0
            
            # CASE A: Low Cardinality (Enum-like)
            if uniqueness_ratio < 0.1 and cardinality < 1000:
                # ClickHouse implies low cardinality strings are fast without index, 
                # or use Set Bloom Filter (not requested in prompt options, so NONE)
                return IndexRecommendation("NONE", 0.8, "Low Cardinality (Scan is fast)", "")

            # CASE B: Long Text / Natural Language -> Inverted
            # "High Cardinality + Long Text"
            if avg_len > 20:
                return IndexRecommendation(
                    "INVERTED", 
                    0.9, 
                    f"High Cardinality ({cardinality}) + Long Text ({avg_len:.1f} chars)", 
                    "TYPE inverted(0) GRANULARITY 1"
                )

            # CASE C: Short Text / Codes -> TokenBF
            # "High Cardinality + Short Text"
            if avg_len <= 20 and uniqueness_ratio > 0.5:
                # Sub-decision: Is it "Tokenizable" (has separators)?
                # If it has few non-alnum chars (like username 'user123'), it might not tokenize well.
                # If it's an email 'user@domain.com', it has separators.
                
                if avg_non_alnum >= 1: # Has separators like @, ., -
                    return IndexRecommendation(
                        "TOKENBF", 
                        0.85, 
                        f"Short Text ({avg_len:.1f}) with Separators (Email/User)", 
                        "TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1"
                    )
                else:
                    # Continuous string (Hash, License Plate, DNA) -> N-Gram
                    # "Substring Search Likely"
                    return IndexRecommendation(
                        "NGRAMBF", 
                        0.80, 
                        f"Short Continuous String ({avg_len:.1f} chars)", 
                        "TYPE ngrambf_v1(4, 32768, 2, 0) GRANULARITY 1"
                    )

            # Default Fallback: Inverted is usually the safest bet for "Search"
            return IndexRecommendation(
                "INVERTED", 
                0.6, 
                "General Purpose Text Search", 
                "TYPE inverted(0) GRANULARITY 1"
            )

        except Exception as e:
            return IndexRecommendation("ERROR", 0.0, str(e), "")

class IndexManager:
    def __init__(self, repo: ClickHouseAdapter):
        self.repo = repo
        self.table = "vault.breach_records"
        self.analyzer = HeuristicAnalyzer(repo)

    def list_indexes(self) -> List[Dict[str, Any]]:
        raw = self.repo.get_indices(self.table)
        # raw: [(name, type, expr, granularity), ...]
        # We need size. 'system.data_skipping_indices' doesn't strictly give size easily per index 
        # without querying system.data_skipping_indices_size (if available) or estimating.
        # ClickHouse stores index files separate.
        # We will list what we have.
        
        indexes = []
        for r in raw:
            indexes.append({
                "name": r[0],
                "type": r[1],
                "column": r[2].split(" ")[0] if " " in r[2] else r[2], # Rough parse of expr
                "granularity": r[3]
            })
        return indexes

    def drop_index(self, column: str) -> None:
        # Find index for column.
        # Naming convention: idx_{column} or idx_{type}_{column}
        # We'll drop anything that looks related or exact match if user provides index name (but user provides col)
        
        current = self.list_indexes()
        targets = [idx['name'] for idx in current if column in idx['name'] or idx['column'] == column]
        
        if not targets:
            log_warning(f"No index found for column '{column}'")
            return

        for idx_name in targets:
            log_info(f"Dropping index: {idx_name}")
            self.repo.client.command(f"ALTER TABLE {self.table} DROP INDEX {idx_name}")

    def apply_index(self, column: str, ddl_def: str) -> None:
        # Check if exists
        idx_name = f"idx_{column}_{ddl_def.split('(')[0].split(' ')[1].lower()}" # rough name: idx_email_inverted
        
        # Simpler name to avoid length issues: idx_{column}
        # But if we want multiple? No, usually one per col.
        idx_name = f"idx_{column}"
        
        log_info(f"Applying Index on {column}: {ddl_def}")
        
        # Drop existing with same name
        self.repo.client.command(f"ALTER TABLE {self.table} DROP INDEX IF EXISTS {idx_name}")
        
        # Add
        try:
            self.repo.client.command(f"ALTER TABLE {self.table} ADD INDEX {idx_name} {column} {ddl_def}")
            # Materialize
            log_info("Materializing index (Background)...")
            self.repo.client.command(f"ALTER TABLE {self.table} MATERIALIZE INDEX {idx_name}")
        except Exception as e:
            log_error(f"Failed to apply index: {e}")

