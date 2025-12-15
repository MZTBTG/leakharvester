from pathlib import Path
from typing import Dict
import polars as pl
import pyarrow as pa
from leakharvester.ports.repository import BreachRepository
from leakharvester.ports.file_storage import FileStorage
from leakharvester.domain.rules import detect_column_mapping, normalize_text_expr, build_search_blob_expr
from leakharvester.domain.schemas import CANONICAL_SCHEMA
from leakharvester.adapters.console import log_info, log_error, log_warning, log_success

class BreachIngestor:
    def __init__(self, repository: BreachRepository, file_storage: FileStorage):
        self.repository = repository
        self.file_storage = file_storage

    def process_file(
        self, 
        input_path: Path, 
        staging_dir: Path, 
        quarantine_dir: Path,
        batch_size: int = 500_000
    ) -> None:
        log_info(f"Iniciando processamento de: {input_path}")
        
        # 1. Lazy Scan
        try:
            lazy_df = self.file_storage.scan_csv(input_path)
        except Exception as e:
            log_error(f"Erro ao inicializar scan para {input_path}: {e}")
            return

        # 2. Schema Normalization
        try:
            # We need to fetch schema/columns to do mapping. 
            # scan_csv is lazy, but we can inspect columns from the plan or fetch 1 row.
            # Polars lazy frame has .columns property which might require schema inference.
            # However, `scan_csv` usually infers schema. 
            columns = lazy_df.collect_schema().names()
        except Exception as e:
             log_error(f"Erro ao inferir schema para {input_path}: {e}")
             self._move_to_quarantine(input_path, quarantine_dir)
             return

        rename_map = detect_column_mapping(columns)
        if not rename_map:
             log_warning(f"Não foi possível mapear colunas críticas em {input_path}. Movendo para quarentena.")
             self._move_to_quarantine(input_path, quarantine_dir)
             return
        
        lazy_df = lazy_df.rename(rename_map)

        # 3. Transformation and Enrichment
        # Ensure we have all target columns, filling missing ones with nulls/literals
        target_cols = CANONICAL_SCHEMA.names()
        
        # Add source_file
        lazy_df = lazy_df.with_columns(pl.lit(input_path.name).alias("source_file"))

        # Add missing columns as empty strings/nulls so we can select them later
        existing_cols = lazy_df.collect_schema().names()
        for col in target_cols:
            if col not in existing_cols and col != "_search_blob" and col != "import_date":
                # breach_date might need special handling, but for now empty string if missing? 
                # Schema says Date. If missing, we might default to null.
                if col == "breach_date":
                     lazy_df = lazy_df.with_columns(pl.lit(None).cast(pl.Date).alias(col))
                else:
                     lazy_df = lazy_df.with_columns(pl.lit("").alias(col))

        # Build _search_blob
        # We assume email, username, password exist now (or are empty strings)
        search_cols = ["email", "username", "password", "hash"]
        # Only use cols that exist
        valid_search_cols = [c for c in search_cols if c in existing_cols or c in rename_map.values()]
        
        lazy_df = lazy_df.with_columns(
             build_search_blob_expr(valid_search_cols)
        )
        
        # Add import_date
        lazy_df = lazy_df.with_columns(pl.lit(None).cast(pl.Datetime).alias("import_date")) # DB default is now(), but Parquet needs type

        # Final Select to enforce schema order
        # We need to cast types to match CANONICAL_SCHEMA
        # Parquet sink will fail if types don't match exactly what we defined or what is inferred.
        # Let's cast explicitly.
        
        exprs = []
        for name, dtype in CANONICAL_SCHEMA.items():
            if name == "import_date":
                 # Let ClickHouse handle default, but for parquet we need a value?
                 # Actually if we pass null, ClickHouse might take it.
                 # But ClickHouse schema has DEFAULT now().
                 # If we send Arrow table, we must match structure.
                 pass
            exprs.append(pl.col(name).cast(dtype))

        lazy_df = lazy_df.select(exprs)

        # 4. Materialize to Parquet
        parquet_path = staging_dir / f"{input_path.stem}.parquet"
        try:
            self.file_storage.write_parquet(lazy_df, parquet_path)
            log_info(f"Arquivo intermediário gerado: {parquet_path}")
        except Exception as e:
            log_error(f"Falha ao converter para Parquet: {e}. Arquivo pode estar muito corrompido.")
            self._move_to_quarantine(input_path, quarantine_dir)
            return

        # 5. Ingest to ClickHouse
        try:
            self._ingest_parquet_to_clickhouse(parquet_path, batch_size)
            log_success(f"Ingestão concluída para {input_path}")
            # Optional: Move original to processed or delete
            # self.file_storage.move_file(input_path, processed_dir) 
            # For now we leave it or user decides.
        except Exception as e:
            log_error(f"Erro na ingestão para o banco: {e}")
            # Parquet is fine but DB rejected it?
            
    def _ingest_parquet_to_clickhouse(self, parquet_path: Path, batch_size: int) -> None:
        for batch in self.file_storage.read_parquet_batches(parquet_path, batch_size):
            table = pa.Table.from_batches([batch])
            self.repository.insert_arrow_batch(table, "breach_records")
            
    def _move_to_quarantine(self, file_path: Path, quarantine_dir: Path) -> None:
        try:
            dest = quarantine_dir / file_path.name
            self.file_storage.move_file(file_path, dest)
            log_warning(f"Arquivo movido para quarentena: {dest}")
        except Exception as e:
            log_error(f"Falha ao mover para quarentena: {e}")
