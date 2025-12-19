from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
import threading
import queue
import polars as pl
import pyarrow as pa
from rich.prompt import Confirm
from leakharvester.ports.repository import BreachRepository
from leakharvester.ports.file_storage import FileStorage
from leakharvester.domain.rules import detect_column_mapping
from leakharvester.domain.schemas import CANONICAL_SCHEMA
from leakharvester.adapters.console import log_info, log_error, log_warning, log_success

class BreachIngestor:
    def __init__(self, repository: BreachRepository, file_storage: FileStorage):
        self.repository = repository
        self.file_storage = file_storage

    def _parse_format_string(self, format_str: str) -> Tuple[str, List[str]]:
        """
        Parses the format string to determine delimiter and column names.
        Example: 'email:pass:doc' -> (':', ['email', 'password', 'document'])
        Example: 'email,pass,null' -> (',', ['email', 'password', 'null'])
        """
        if format_str == "auto" or format_str == "email:pass":
             return ":", ["email", "password"]

        delimiters = [":", ",", ";", "|"]
        detected_delimiter = ":" # Default
        
        # Heuristic: Count delimiters in the format string itself
        counts = {d: format_str.count(d) for d in delimiters}
        # Pick the one with the most occurrences
        best_delimiter = max(counts, key=counts.get)
        
        if counts[best_delimiter] > 0:
            detected_delimiter = best_delimiter
            
        columns = [c.strip() for c in format_str.split(detected_delimiter)]
        
        # Normalize specific common abbreviations
        normalized_cols = []
        for c in columns:
            if c == "pass": normalized_cols.append("password")
            elif c == "user": normalized_cols.append("username")
            else: normalized_cols.append(c)
            
        return detected_delimiter, normalized_cols

    def _validate_and_sync_schema(self, columns: List[str]) -> bool:
        """
        Checks if columns exist in ClickHouse. Prompts user to create missing ones.
        Returns False if user aborts.
        """
        # Get existing columns from DB
        try:
            # We assume table is vault.breach_records based on main.py
            existing_cols = set(self.repository.get_columns("vault.breach_records"))
        except Exception as e:
            log_error(f"Failed to fetch schema from DB: {e}")
            return False

        # Filter out 'null' placeholder and existing cols
        missing_cols = []
        for col in columns:
            if col == "null": continue
            if col not in existing_cols:
                missing_cols.append(col)
        
        if not missing_cols:
            return True

        log_warning(f"The following columns are missing in the database: {missing_cols}")
        if Confirm.ask(f"Do you want to add these {len(missing_cols)} columns to the database schema?", default=True):
            try:
                for col in missing_cols:
                    log_info(f"Adding column '{col}'...")
                    self.repository.add_column("vault.breach_records", col)
                log_success("Schema updated successfully.")
                return True
            except Exception as e:
                log_error(f"Failed to update schema: {e}")
                return False
        else:
            log_error("Ingestion aborted by user due to schema mismatch.")
            return False

    def process_file(
        self, 
        input_path: Path, 
        staging_dir: Path, 
        quarantine_dir: Path,
        batch_size: int = 500_000,
        format: str = "auto",
        no_check: bool = False,
        custom_source_name: Optional[str] = None,
        num_workers: int = 1
    ) -> None:
        log_info(f"Iniciando processamento de: {input_path} [Format: {format}, NoCheck: {no_check}, Workers: {num_workers}]")
        
        # If format is explicit (not auto), skip detection and go straight to streaming
        if format != "auto":
             self._process_raw_streaming(input_path, staging_dir, batch_size, format=format, no_check=no_check, custom_source_name=custom_source_name, num_workers=num_workers)
             return

        # Initial peek to detect columns/schema
        try:
            preview_df = pl.read_csv(input_path, n_rows=100, ignore_errors=True)
            columns = preview_df.columns
        except Exception as e:
            log_error(f"Erro ao ler cabeçalho de {input_path}: {e}")
            return

        rename_map = detect_column_mapping(columns)
        
        # If mapping fails or it looks raw, fallback to Raw Streaming
        if not rename_map:
             if "raw_line" in columns or len(columns) == 1:
                 log_warning(f"Modo RAW detectado para {input_path}. Alternando para Ingestão em Streaming.")
                 self._process_raw_streaming(input_path, staging_dir, batch_size, format=format, no_check=no_check, custom_source_name=custom_source_name, num_workers=num_workers)
                 return
             else:
                 log_warning(f"Não foi possível mapear colunas críticas em {input_path}. Movendo para quarentena.")
                 self._move_to_quarantine(input_path, quarantine_dir)
                 return

        # --- STREAMING STRUCTURED INGESTION ---
        chunk_idx = 0
        
        # Threading Setup
        upload_queue = queue.Queue(maxsize=num_workers * 2)
        stop_event = threading.Event()
        error_container = []

        # Start Shared Workers
        workers = []
        for _ in range(num_workers):
            t = threading.Thread(
                target=self._ingestion_worker, 
                args=(upload_queue, stop_event, error_container), 
                daemon=True
            )
            t.start()
            workers.append(t)

        try:
            reader = pl.read_csv_batched(
                input_path, 
                batch_size=batch_size, 
                ignore_errors=True,
                infer_schema_length=10000,
                low_memory=True
            )
            
            while True:
                if stop_event.is_set():
                    break
                
                batches = reader.next_batches(1)
                if not batches:
                    break
                
                df = batches[0]
                chunk_idx += 1
                
                # --- TRANSFORMATION LOGIC (Per Batch) ---
                valid_renames = {k: v for k, v in rename_map.items() if k in df.columns}
                df = df.rename(valid_renames)
                
                # Add Metadata Columns
                source_label = custom_source_name or input_path.name
                df = df.with_columns(pl.lit(source_label).alias("source_file"))

                # Ensure canonical cols exist (email, username, etc from Schema)
                target_cols = CANONICAL_SCHEMA.names()
                current_cols = df.columns
                
                cols_to_add = []
                for col in target_cols:
                    if col not in current_cols:
                        if col == "breach_date":
                            cols_to_add.append(pl.lit(None).cast(pl.Date).alias(col))
                        elif col == "import_date":
                            cols_to_add.append(pl.lit(None).cast(pl.Datetime).alias(col))
                        else:
                            cols_to_add.append(pl.lit("").alias(col))
                
                if cols_to_add:
                    df = df.with_columns(cols_to_add)

                # Final Select & Cast
                exprs = [pl.col(name).cast(dtype) for name, dtype in CANONICAL_SCHEMA.items()]
                df = df.select(exprs)

                # --- PUSH TO WORKER ---
                try:
                    arrow_table = df.to_arrow()
                    self._push_to_worker(upload_queue, (arrow_table, "breach_records"), stop_event, error_container)
                except Exception as e:
                    log_error(f"Failed to process structured chunk {chunk_idx}: {e}")
                    q_path = staging_dir.parent / "quarantine" / f"failed_struct_{input_path.stem}_{chunk_idx}.parquet"
                    df.write_parquet(q_path)

            # End of Stream
            if not stop_event.is_set():
                for _ in range(num_workers):
                    self._push_to_worker(upload_queue, None, stop_event, error_container)
                
                upload_queue.join()
                for t in workers:
                    t.join()

            if error_container:
                raise error_container[0]

            log_success(f"Ingestão Estruturada concluída para {input_path}. Total chunks: {chunk_idx}")

        except KeyboardInterrupt:
            log_warning("Ingestão interrompida pelo usuário.")
            stop_event.set()
            raise
        except Exception as e:
            log_error(f"Erro durante processamento de {input_path}: {e}")
            self._move_to_quarantine(input_path, quarantine_dir)
            stop_event.set()
        finally:
             for t in workers:
                t.join(timeout=2.0)

    def process_stream(self, stream: Any, staging_dir: Path, quarantine_dir: Path, batch_size: int, source_name: str = "stdin", format: str = "auto", no_check: bool = False, num_workers: int = 1) -> None:
        """
        Ingests data from a stream (stdin/pipe) using the dynamic logic.
        """
        import warnings
        import time
        warnings.filterwarnings("ignore", message="CSV malformed")
        
        chunk_idx = 0
        total_rows = 0
        start_time = time.time()
        
        # 1. Parse Format & Validate Schema
        delimiter, columns = self._parse_format_string(format)
        if not self._validate_and_sync_schema(columns):
            return

        # 2. Setup Workers
        upload_queue = queue.Queue(maxsize=num_workers * 2)
        stop_event = threading.Event()
        error_container = []

        workers = []
        for _ in range(num_workers):
            t = threading.Thread(
                target=self._ingestion_worker, 
                args=(upload_queue, stop_event, error_container), 
                daemon=True
            )
            t.start()
            workers.append(t)
        
        log_info(f"Iniciando ingestão via Stream ({source_name}) [Format: {format}] [Delim: '{delimiter}'] [Cols: {columns}]")
        
        try:
            for df in self.file_storage.read_stream_batched(stream, batch_size=batch_size):
                if stop_event.is_set():
                    break 

                chunk_idx += 1
                total_rows += df.height
                
                if chunk_idx % 10 == 0:
                     elapsed = time.time() - start_time
                     log_info(f"Processando chunk {chunk_idx} ({total_rows} linhas)... {elapsed:.2f}s")
                
                # --- Dynamic Split Logic ---
                num_cols = len(columns)
                df = df.with_columns(
                    pl.col("raw_line").str.splitn(delimiter, num_cols).alias("split_parts")
                )
                
                # Extract columns
                exprs = []
                for i, col_name in enumerate(columns):
                    if col_name == "null":
                        continue
                    exprs.append(
                        pl.col("split_parts").struct.field(f"field_{i}").alias(col_name)
                    )
                
                df = df.with_columns(exprs)
                
                # Filter rows where primary identity is null (e.g. email)
                if "email" in columns:
                     df = df.filter(pl.col("email").is_not_null() & (pl.col("email") != ""))
                     
                     if not no_check:
                        validation_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
                        bad_df = df.filter(~pl.col("email").str.contains(validation_pattern))
                        
                        if bad_df.height > 0:
                             q_path = quarantine_dir / f"quarantine_{source_name}_{chunk_idx}.parquet"
                             try:
                                 bad_df.select(["raw_line"]).write_parquet(q_path, compression="zstd")
                             except Exception: pass
                        
                        df = df.filter(pl.col("email").str.contains(validation_pattern))

                if df.height == 0:
                    continue

                # Add Metadata
                df = df.with_columns([
                    pl.lit(source_name).alias("source_file"),
                    pl.lit(None).cast(pl.Date).alias("breach_date"),
                    pl.lit(None).cast(pl.Datetime).alias("import_date")
                ])
                
                # Fill missing canonical columns
                target_cols = CANONICAL_SCHEMA.names()
                current_cols = set(df.columns)
                missing_exprs = []
                for tc in target_cols:
                    if tc not in current_cols:
                         if tc == "breach_date" or tc == "import_date": continue 
                         missing_exprs.append(pl.lit("").alias(tc))
                
                if missing_exprs:
                    df = df.with_columns(missing_exprs)
                
                # Select Final Columns
                cols_to_keep = list(set(CANONICAL_SCHEMA.names()) | (set(columns) - {"null"}))
                final_df = df.select([c for c in cols_to_keep if c in df.columns])
                
                # Push
                try:
                    arrow_table = final_df.to_arrow()
                    self._push_to_worker(upload_queue, (arrow_table, "breach_records"), stop_event, error_container)
                except Exception as e:
                    log_error(f"Failed to convert chunk {chunk_idx}: {e}")
            
            # End of Stream
            if not stop_event.is_set():
                for _ in range(num_workers):
                    self._push_to_worker(upload_queue, None, stop_event, error_container)
                upload_queue.join()
                for t in workers:
                    t.join()

            if error_container:
                raise error_container[0]
            
            total_time = time.time() - start_time
            log_success(f"Ingestão via Stream concluída. Total chunks: {chunk_idx} | Linhas: {total_rows} | Tempo: {total_time:.2f}s")
            
        except KeyboardInterrupt:
            log_warning("Ingestão interrompida pelo usuário.")
            stop_event.set()
            raise
        except Exception as e:
            log_error(f"Erro durante ingestão de stream: {e}")
            stop_event.set()
        finally:
            for t in workers:
                t.join(timeout=2.0)

    def _process_raw_streaming(self, input_path: Path, staging_dir: Path, batch_size: int, format: str = "auto", no_check: bool = False, custom_source_name: Optional[str] = None, num_workers: int = 1) -> None:
        """
        Ingests a raw file in chunks using Parallel Producer-Consumer pattern with Dynamic Format support.
        """
        import time
        chunk_idx = 0
        total_rows = 0
        start_time = time.time()
        
        # 1. Parse Format & Validate
        delimiter, columns = self._parse_format_string(format)
        if not self._validate_and_sync_schema(columns):
            return

        # 2. Workers
        upload_queue = queue.Queue(maxsize=num_workers * 2)
        stop_event = threading.Event()
        error_container = []

        workers = []
        for _ in range(num_workers):
            t = threading.Thread(
                target=self._ingestion_worker, 
                args=(upload_queue, stop_event, error_container), 
                daemon=True
            )
            t.start()
            workers.append(t)
        
        log_info(f"Iniciando Ingestão Streaming de {input_path.name} [Delim: '{delimiter}'] [Cols: {len(columns)}]")

        try:
            for batch_data in self.file_storage.read_lines_batched(input_path, batch_size=batch_size):
                if stop_event.is_set():
                    break

                chunk_idx += 1
                
                if isinstance(batch_data, pl.DataFrame):
                    df = batch_data
                    if "raw_line" not in df.columns:
                         df.columns = ["raw_line"]
                else:
                    df = pl.DataFrame({"raw_line": batch_data})

                total_rows += df.height
                
                if chunk_idx % 10 == 0:
                     elapsed = time.time() - start_time
                     log_info(f"Processando chunk {chunk_idx} ({total_rows} linhas)... {elapsed:.2f}s")

                # --- Dynamic Split Logic ---
                num_cols = len(columns)
                df = df.with_columns(
                    pl.col("raw_line").str.splitn(delimiter, num_cols).alias("split_parts")
                )

                # Map to Columns
                exprs = []
                for i, col_name in enumerate(columns):
                    if col_name == "null": continue
                    exprs.append(
                        pl.col("split_parts").struct.field(f"field_{i}").alias(col_name)
                    )
                df = df.with_columns(exprs)

                # Validation
                if "email" in columns:
                     df = df.filter(pl.col("email").is_not_null() & (pl.col("email") != ""))
                     if not no_check:
                        validation_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
                        bad_df = df.filter(~pl.col("email").str.contains(validation_pattern))
                        if bad_df.height > 0:
                             q_path = staging_dir.parent / "quarantine" / f"quarantine_{input_path.stem}_{chunk_idx}.parquet"
                             try:
                                 bad_df.select(["raw_line"]).write_parquet(q_path, compression="zstd")
                             except Exception: pass
                        df = df.filter(pl.col("email").str.contains(validation_pattern))

                if df.height == 0:
                    continue

                # Metadata
                source_label = custom_source_name or input_path.name
                df = df.with_columns([
                    pl.lit(source_label).alias("source_file"),
                    pl.lit(None).cast(pl.Date).alias("breach_date"),
                    pl.lit(None).cast(pl.Datetime).alias("import_date")
                ])
                
                # Missing Canonical Columns
                target_cols = CANONICAL_SCHEMA.names()
                current_cols = set(df.columns)
                missing_exprs = []
                for tc in target_cols:
                    if tc not in current_cols:
                         if tc == "breach_date" or tc == "import_date": continue
                         missing_exprs.append(pl.lit("").alias(tc))
                if missing_exprs:
                    df = df.with_columns(missing_exprs)
                
                # Select Final Columns
                cols_to_keep = list(set(CANONICAL_SCHEMA.names()) | (set(columns) - {"null"}))
                final_df = df.select([c for c in cols_to_keep if c in df.columns])
                
                # Push
                try:
                    arrow_table = final_df.to_arrow()
                    self._push_to_worker(upload_queue, (arrow_table, "breach_records"), stop_event, error_container)
                except Exception as e:
                    log_error(f"Failed to ingest chunk {chunk_idx}: {e}")
                    q_path = staging_dir.parent / "quarantine" / f"failed_ingest_{input_path.stem}_{chunk_idx}.parquet"
                    df.write_parquet(q_path)
            
            # End
            if not stop_event.is_set():
                for _ in range(num_workers):
                    self._push_to_worker(upload_queue, None, stop_event, error_container)
                upload_queue.join()
                for t in workers:
                    t.join()
            
            if error_container:
                raise error_container[0]

            total_time = time.time() - start_time
            log_success(f"Ingestão Paralela via Streaming concluída para {input_path}. Total chunks: {chunk_idx} | Linhas: {total_rows} | Tempo: {total_time:.2f}s")
            
        except KeyboardInterrupt:
            log_warning("Ingestão interrompida pelo usuário.")
            stop_event.set()
            raise
        except Exception as e:
            log_error(f"Erro durante streaming raw de {input_path}: {e}")
            self._move_to_quarantine(input_path, staging_dir.parent / "quarantine")
            stop_event.set()
        finally:
            for t in workers:
                t.join(timeout=2.0)

    def _ingestion_worker(self, q: queue.Queue, error_event: threading.Event, error_container: list) -> None:
        """Background worker to consume Arrow tables and insert into ClickHouse."""
        try:
            while True:
                try:
                    item = q.get(timeout=1.0)
                except queue.Empty:
                    if error_event.is_set():
                        break
                    continue

                if item is None: # Sentinel
                    q.task_done()
                    break
                
                table, table_name = item
                try:
                    self.repository.insert_arrow_batch(table, table_name)
                except Exception as e:
                    log_error(f"Worker Upload Failed: {e}")
                    error_container.append(e)
                    error_event.set()
                    
                    try:
                        while True:
                            q.get_nowait()
                            q.task_done()
                    except queue.Empty:
                        pass
                    break 
                finally:
                    if not error_event.is_set():
                        q.task_done()
                    
        except Exception as e:
            if not error_container:
                error_container.append(e)
            error_event.set()

    def _push_to_worker(self, q, item, stop_event, error_container):
        while not stop_event.is_set():
            try:
                q.put(item, timeout=0.5)
                return
            except queue.Full:
                continue
        
        if error_container:
            raise error_container[0]
        raise RuntimeError("Worker stopped unexpectedly")
    
    def _ingest_parquet_to_clickhouse(self, parquet_path: Path, batch_size: int) -> None:
        """
        Reads a Parquet file and ingests chunks using the shared worker pattern.
        """
        # --- THREADING SETUP ---
        upload_queue = queue.Queue(maxsize=3)
        stop_event = threading.Event()
        error_container = []

        # Start Shared Worker
        worker_thread = threading.Thread(
            target=self._ingestion_worker, 
            args=(upload_queue, stop_event, error_container), 
            daemon=True
        )
        worker_thread.start()
        
        try:
            for batch in self.file_storage.read_parquet_batches(parquet_path, batch_size):
                if stop_event.is_set():
                    break
                
                try:
                    table = pa.Table.from_batches([batch])
                    self._push_to_worker(upload_queue, (table, "breach_records"), stop_event, error_container)
                except Exception as e:
                    log_error(f"Failed to process parquet batch: {e}")
                    raise e
            
            # End
            if not stop_event.is_set():
                self._push_to_worker(upload_queue, None, stop_event, error_container)
                upload_queue.join()
            
            if error_container:
                raise error_container[0]
                
        except Exception as e:
            stop_event.set()
            raise e

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
                        pl.lit("").alias("username"),
                        pl.lit("").alias("password"),
                        pl.lit(None).cast(pl.Date).alias("breach_date"),
                        pl.lit(None).cast(pl.Datetime).alias("import_date")
                    ])
                    
                    valid_df = valid_df.select(CANONICAL_SCHEMA.names())
                    
                    # Convert to Arrow and Insert
                    table = valid_df.to_arrow()
                    self.repository.insert_arrow_batch(table, "breach_records")
                    
                    total_recovered += valid_df.height
                    log_info(f"Recuperado {valid_df.height} linhas de {q_file.name}")
                
                q_file.unlink()
                
            except Exception as e:
                log_error(f"Erro ao reparar {q_file}: {e}")
        
        log_success(f"Reparo concluído. Total recuperado: {total_recovered} registros.")
