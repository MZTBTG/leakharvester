from pathlib import Path
from typing import Dict, Any
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
        batch_size: int = 500_000,
        format: str = "auto",
        no_check: bool = False
    ) -> None:
        log_info(f"Iniciando processamento de: {input_path} [Format: {format}, NoCheck: {no_check}]")
        
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
             if "raw_line" in columns or len(columns) == 1:
                 log_warning(f"Modo RAW detectado para {input_path}. Alternando para Ingestão em Streaming (Baixo Consumo de RAM).")
                 self._process_raw_streaming(input_path, staging_dir, batch_size, format=format, no_check=no_check)
                 return
             else:
                 log_warning(f"Não foi possível mapear colunas críticas em {input_path}. Movendo para quarentena.")
                 self._move_to_quarantine(input_path, quarantine_dir)
                 return
        
        # Standard Structured Mode (Logic continues here for CSVs)
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
        lazy_df = lazy_df.with_columns(pl.lit(None).cast(pl.Datetime).alias("import_date"))

        # Final Select to enforce schema order
        exprs = []
        for name, dtype in CANONICAL_SCHEMA.items():
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
        except Exception as e:
            log_error(f"Erro na ingestão para o banco: {e}")

    def process_stream(self, stream: Any, staging_dir: Path, quarantine_dir: Path, batch_size: int, source_name: str = "stdin", format: str = "auto", no_check: bool = False) -> None:
        """
        Ingests data from a stream (stdin/pipe). Always assumes Raw Line mode.
        If format="email:pass", skips regex validation for speed (Optimistic Mode).
        If no_check=True, skips even simple validation (Fastest).
        """
        import warnings
        # Suppress Polars "CSV malformed" warnings typical for parallel parsing of ragged data
        warnings.filterwarnings("ignore", message="CSV malformed")
        
        chunk_idx = 0
        email_pattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
        
        log_info(f"Iniciando ingestão via Stream ({source_name}) [Format: {format}]...")
        
        try:
            for df in self.file_storage.read_stream_batched(stream, batch_size=batch_size):
                chunk_idx += 1
                if chunk_idx % 10 == 0:
                     log_info(f"Processando chunk {chunk_idx} ({df.height} linhas)...")
                
                # Split raw_line into max 2 parts
                df = df.with_columns(
                    pl.col("raw_line").str.splitn(":", 2).alias("split_parts")
                )
                
                df = df.with_columns([
                    pl.col("split_parts").struct.field("field_0").alias("part_0"),
                    pl.col("split_parts").struct.field("field_1").alias("part_1")
                ])
                
                if format == "email:pass":
                    # Fast Path with Validation
                    df = df.with_columns(
                        pl.col("part_0").alias("email"),
                        pl.col("part_1").fill_null("").alias("password")
                    )
                    
                    if not no_check:
                        # Validation Regex (Simple/Fast)
                        validation_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
                        
                        # Split into Good and Bad
                        good_df = df.filter(pl.col("email").str.contains(validation_pattern))
                        bad_df = df.filter(~pl.col("email").str.contains(validation_pattern))
                        
                        # Handle Bad Rows (Quarantine)
                        if bad_df.height > 0:
                            q_path = quarantine_dir / f"quarantine_{source_name}_{chunk_idx}.parquet"
                            try:
                                bad_df.select(["raw_line"]).write_parquet(q_path, compression="zstd")
                            except Exception as e:
                                log_error(f"Failed to write quarantine chunk: {e}")
                        
                        df = good_df
                    # else: no_check is True, accept all rows as-is (except null email which is filtered later)
                    
                else:
                    # Robust Path: Hybrid Split/Regex
                    df = df.with_columns(
                         pl.when(pl.col("part_0").str.contains(email_pattern))
                           .then(pl.col("part_0"))
                           .otherwise(pl.col("raw_line").str.extract(email_pattern, 1))
                           .alias("email"),
                         
                         pl.when(pl.col("part_0").str.contains(email_pattern))
                           .then(pl.col("part_1"))
                           .otherwise(pl.lit("")) 
                           .alias("password")
                    )
                    # In robust mode, we already filtered null emails later, 
                    # but we could also quarantine them if we wanted.
                    # For now, keep existing behavior for 'auto' mode.

                df = df.filter(pl.col("email").is_not_null())
                
                if df.height == 0:
                    continue

                df = df.with_columns([
                    pl.lit(source_name).alias("source_file"),
                    pl.col("raw_line").str.to_lowercase().alias("_search_blob"),
                    pl.lit("").alias("username"),
                    pl.lit("").alias("hash"),
                    pl.lit("").alias("salt"),
                    pl.lit(None).cast(pl.Date).alias("breach_date"),
                    pl.lit(None).cast(pl.Datetime).alias("import_date")
                ])
                
                df = df.select(CANONICAL_SCHEMA.names())
                
                # Write Chunk to Parquet
                chunk_path = staging_dir / f"{source_name}_chunk_{chunk_idx}.parquet"
                try:
                    df.write_parquet(chunk_path, compression="zstd")
                    self._ingest_parquet_to_clickhouse(chunk_path, batch_size)
                finally:
                    if chunk_path.exists():
                        chunk_path.unlink()
            
            log_success(f"Ingestão via Stream concluída. Total chunks: {chunk_idx}")
            
        except KeyboardInterrupt:
            log_warning("Ingestão interrompida pelo usuário.")
            raise
        except Exception as e:
            log_error(f"Erro durante ingestão de stream: {e}")

    def _process_raw_streaming(self, input_path: Path, staging_dir: Path, batch_size: int, format: str = "auto", no_check: bool = False) -> None:
        """
        Ingests a raw file in chunks to minimize memory usage.
        Reads lines -> Extract Regex -> Write Temp Parquet -> Ingest -> Delete Temp Parquet
        """
        chunk_idx = 0
        email_pattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
        
        try:
            # batch_size passed here is from config (e.g. 5,000,000)
            for batch_data in self.file_storage.read_lines_batched(input_path, batch_size=batch_size):
                chunk_idx += 1
                
                # Check if batch_data is already a DataFrame (optimized path) or list (fallback)
                if isinstance(batch_data, pl.DataFrame):
                    df = batch_data
                    if "raw_line" not in df.columns:
                         # Ensure column name consistency
                         df.columns = ["raw_line"]
                else:
                    df = pl.DataFrame({"raw_line": batch_data})

                log_info(f"Processando chunk {chunk_idx} ({df.height} linhas)...")

                if format == "email:pass":
                    # --- FAST PATH ---
                    # 1. Split by ':' 
                    df = df.with_columns(
                        pl.col("raw_line").str.splitn(":", 2).alias("split_parts")
                    )
                    df = df.with_columns(
                        pl.col("split_parts").struct.field("field_0").alias("email"),
                        pl.col("split_parts").struct.field("field_1").fill_null("").alias("password")
                    )
                    
                    if not no_check:
                        # 2. Simple Validation (Optimistic)
                        good_df = df.filter(
                            pl.col("email").str.contains("@", literal=True) & 
                            (pl.col("email").str.len_bytes() > 5)
                        )
                        
                        # Quarantine Bad Rows
                        bad_df = df.filter(~(pl.col("email").str.contains("@", literal=True) & (pl.col("email").str.len_bytes() > 5)))
                        
                        if bad_df.height > 0:
                             q_path = staging_dir.parent / "quarantine" / f"quarantine_{input_path.stem}_{chunk_idx}.parquet"
                             try:
                                 bad_df.select(["raw_line"]).write_parquet(q_path, compression="zstd")
                             except Exception: pass

                        df = good_df
                    # else: no_check is True, skip validation and quarantine

                else:
                    # --- ROBUST PATH (Regex) ---
                    df = df.with_columns(
                        pl.col("raw_line").str.extract(email_pattern, 1).alias("email")
                    ).filter(pl.col("email").is_not_null())
                
                if df.height == 0:
                    continue

                df = df.with_columns([
                    pl.lit(input_path.name).alias("source_file"),
                    # Search blob includes raw line to capture context
                    pl.col("raw_line").str.to_lowercase().alias("_search_blob"),
                    # Create username col defaulting to empty
                    pl.lit("").alias("username"),
                    pl.lit("").alias("hash"),
                    pl.lit("").alias("salt"),
                    pl.lit(None).cast(pl.Date).alias("breach_date"),
                    pl.lit(None).cast(pl.Datetime).alias("import_date")
                ])
                
                # Select canonical columns
                df = df.select(CANONICAL_SCHEMA.names())
                
                # Write Chunk to Parquet
                chunk_path = staging_dir / f"{input_path.stem}_chunk_{chunk_idx}.parquet"
                try:
                    df.write_parquet(chunk_path, compression="zstd")
                    
                    # Ingest Chunk
                    self._ingest_parquet_to_clickhouse(chunk_path, batch_size)
                finally:
                    # Always try to remove the chunk, success or failure/interruption
                    if chunk_path.exists():
                        chunk_path.unlink()
            
            log_success(f"Ingestão via Streaming concluída para {input_path}. Total chunks: {chunk_idx}")
            
        except KeyboardInterrupt:
            log_warning("Ingestão interrompida pelo usuário.")
            # Verify if last chunk exists and delete it
            # (Handled by finally block of the loop iteration if interrupt happened inside try)
            # But if interrupt happened outside try (e.g. during Polars processing), we can't easily know the path.
            # We can rely on the fact that we clean up immediately.
            # But to be safe, we could register a cleanup list.
            raise
        except Exception as e:
            log_error(f"Erro durante streaming raw de {input_path}: {e}")
            self._move_to_quarantine(input_path, staging_dir.parent / "quarantine")

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

    def repair_quarantine(self, quarantine_dir: Path, staging_dir: Path) -> None:
        """
        Scans quarantine directory, tries to extract emails using heavy regex, and ingests recovered data.
        """
        files = list(quarantine_dir.glob("*.parquet"))
        if not files:
            log_info("Nenhum arquivo encontrado na quarentena.")
            return
            
        log_info(f"Iniciando reparo de {len(files)} arquivos em quarentena...")
        
        email_pattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
        total_recovered = 0
        
        for q_file in files:
            try:
                df = pl.read_parquet(q_file)
                if df.height == 0:
                    q_file.unlink()
                    continue
                
                # Apply Heavy Regex Extraction
                df = df.with_columns(
                    pl.col("raw_line").str.extract(email_pattern, 1).alias("email")
                )
                
                # Filter Valid
                valid_df = df.filter(pl.col("email").is_not_null())
                
                if valid_df.height > 0:
                    # Enriched with missing cols
                    valid_df = valid_df.with_columns([
                        pl.lit(q_file.name).alias("source_file"),
                        pl.col("raw_line").str.to_lowercase().alias("_search_blob"),
                        # We don't know password/user, so leave empty or extract more?
                        # For repair, simple email capture is good enough.
                        pl.lit("").alias("username"),
                        pl.lit("").alias("password"),
                        pl.lit("").alias("hash"),
                        pl.lit("").alias("salt"),
                        pl.lit(None).cast(pl.Date).alias("breach_date"),
                        pl.lit(None).cast(pl.Datetime).alias("import_date")
                    ])
                    
                    valid_df = valid_df.select(CANONICAL_SCHEMA.names())
                    
                    # Convert to Arrow and Insert
                    table = valid_df.to_arrow()
                    self.repository.insert_arrow_batch(table, "breach_records")
                    
                    total_recovered += valid_df.height
                    log_info(f"Recuperado {valid_df.height} linhas de {q_file.name}")
                
                # Delete processed quarantine file
                q_file.unlink()
                
            except Exception as e:
                log_error(f"Erro ao reparar {q_file}: {e}")
        
        log_success(f"Reparo concluído. Total recuperado: {total_recovered} registros.")
