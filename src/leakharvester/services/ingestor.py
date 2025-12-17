from pathlib import Path
from typing import Dict, Any, Optional
import threading
import queue
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
        
        # Initial peek to detect columns/schema
        try:
            # Read just the header/first few lines to detect schema
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
                 self._process_raw_streaming(input_path, staging_dir, batch_size, format=format, no_check=no_check)
                 return
             else:
                 log_warning(f"Não foi possível mapear colunas críticas em {input_path}. Movendo para quarentena.")
                 self._move_to_quarantine(input_path, quarantine_dir)
                 return

        # --- STREAMING STRUCTURED INGESTION ---
        chunk_idx = 0
        
        # Threading Setup
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
            # Use the iterator to stream batches
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
                # 1. Rename
                # Only rename columns that exist in this batch (safety)
                valid_renames = {k: v for k, v in rename_map.items() if k in df.columns}
                df = df.rename(valid_renames)
                
                # 2. Add Missing/Metadata Columns
                target_cols = CANONICAL_SCHEMA.names()
                
                # Add source_file
                df = df.with_columns(pl.lit(input_path.name).alias("source_file"))

                # Ensure all target columns exist
                current_cols = df.columns
                cols_to_add = []
                for col in target_cols:
                    if col not in current_cols and col != "_search_blob":
                        if col == "breach_date":
                            cols_to_add.append(pl.lit(None).cast(pl.Date).alias(col))
                        elif col == "import_date":
                            cols_to_add.append(pl.lit(None).cast(pl.Datetime).alias(col))
                        else:
                            cols_to_add.append(pl.lit("").alias(col))
                
                if cols_to_add:
                    df = df.with_columns(cols_to_add)

                # 3. Build Search Blob
                search_cols = ["email", "username", "password", "hash"]
                valid_search_cols = [c for c in search_cols if c in df.columns]
                
                df = df.with_columns(
                    build_search_blob_expr(valid_search_cols)
                )
                
                # 4. Final Select & Cast
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
                self._push_to_worker(upload_queue, None, stop_event, error_container)
                upload_queue.join()
                worker_thread.join()

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

    def _ingestion_worker(self, q: queue.Queue, error_event: threading.Event, error_container: list) -> None:
        """Background worker to consume Arrow tables and insert into ClickHouse."""
        try:
            while True:
                try:
                    # Check periodically to allow exit if main thread sets error_event
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
                    
                    # Drain queue to unblock producer
                    try:
                        while True:
                            q.get_nowait()
                            q.task_done()
                    except queue.Empty:
                        pass
                    # We continue loop to hit error_event check or break, 
                    # but actually we should stop consuming if error occurred to be safe.
                    break 
                finally:
                    # Safe call if we haven't already drained it (which calls task_done)
                    # But if we drained, we called task_done on future items. 
                    # For CURRENT item, we must call task_done.
                    # The drain logic is tricky. 
                    # Simpler logic: Call task_done for THIS item. Then if error, drain rest.
                    if not error_event.is_set():
                        q.task_done()
                    # If error set, we might have drained. Just leave it.
                    
        except Exception as e:
            # Catch-all for worker internal logic crash
            if not error_container:
                error_container.append(e)
            error_event.set()

    def _push_to_worker(self, q, item, stop_event, error_container):
        """
        Safely pushes to queue. Prevents deadlock if worker dies.
        """
        while not stop_event.is_set():
            try:
                q.put(item, timeout=0.5)
                return
            except queue.Full:
                continue
        
        # If we exit loop, worker is dead
        if error_container:
            raise error_container[0]
        raise RuntimeError("Worker stopped unexpectedly")

    def process_stream(self, stream: Any, staging_dir: Path, quarantine_dir: Path, batch_size: int, source_name: str = "stdin", format: str = "auto", no_check: bool = False) -> None:
        """
        Ingests data from a stream (stdin/pipe) using a Producer-Consumer thread pattern.
        Producer (Main Thread): Reads stream -> Parsing -> Validation -> Queue
        Consumer (Worker Thread): Queue -> ClickHouse Upload
        """
        import warnings
        
        # Suppress Polars "CSV malformed" warnings typical for parallel parsing of ragged data
        warnings.filterwarnings("ignore", message="CSV malformed")
        
        chunk_idx = 0
        email_pattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
        
        # --- THREADING SETUP ---
        # Queue Maxsize = 3 batches (~3GB - 6GB RAM buffer)
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
        
        log_info(f"Iniciando ingestão Paralela (Producer-Consumer) via Stream ({source_name}) [Format: {format}]...")
        
        try:
            for df in self.file_storage.read_stream_batched(stream, batch_size=batch_size):
                if stop_event.is_set():
                    break # Error happened

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
                        good_df = df.filter(pl.col("email").str.contains(validation_pattern))
                        bad_df = df.filter(~pl.col("email").str.contains(validation_pattern))
                        
                        if bad_df.height > 0:
                            q_path = quarantine_dir / f"quarantine_{source_name}_{chunk_idx}.parquet"
                            try:
                                bad_df.select(["raw_line"]).write_parquet(q_path, compression="zstd")
                            except Exception: pass
                        
                        df = good_df
                    
                else:
                    # Robust Path
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
                
                # Conversion to Arrow (Main Thread)
                try:
                    arrow_table = df.to_arrow()
                    # Safe Push to shared worker queue
                    self._push_to_worker(upload_queue, (arrow_table, "breach_records"), stop_event, error_container)
                except Exception as e:
                    log_error(f"Failed to convert chunk {chunk_idx}: {e}")
            
            # End of Stream
            if not stop_event.is_set():
                self._push_to_worker(upload_queue, None, stop_event, error_container)
                upload_queue.join()
            
            if error_container:
                raise error_container[0]

            log_success(f"Ingestão Paralela via Stream concluída. Total chunks: {chunk_idx}")
            
        except KeyboardInterrupt:
            log_warning("Ingestão interrompida pelo usuário.")
            stop_event.set()
            raise
        except Exception as e:
            log_error(f"Erro durante ingestão de stream: {e}")
            stop_event.set()


    def _process_raw_streaming(self, input_path: Path, staging_dir: Path, batch_size: int, format: str = "auto", no_check: bool = False) -> None:
        """
        Ingests a raw file in chunks using Parallel Producer-Consumer pattern.
        Main Thread: Reads lines -> Extract Regex -> Convert to Arrow
        Worker Thread: Uploads Arrow Batch to ClickHouse
        """
        chunk_idx = 0
        email_pattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
        
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
            # batch_size passed here is from config (e.g. 5,000,000)
            for batch_data in self.file_storage.read_lines_batched(input_path, batch_size=batch_size):
                if stop_event.is_set():
                    break

                chunk_idx += 1
                
                # Check if batch_data is already a DataFrame (optimized path) or list (fallback)
                if isinstance(batch_data, pl.DataFrame):
                    df = batch_data
                    if "raw_line" not in df.columns:
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
                    pl.col("raw_line").str.to_lowercase().alias("_search_blob"),
                    pl.lit("").alias("username"),
                    pl.lit("").alias("password"),
                    pl.lit("").alias("hash"),
                    pl.lit("").alias("salt"),
                    pl.lit(None).cast(pl.Date).alias("breach_date"),
                    pl.lit(None).cast(pl.Datetime).alias("import_date")
                ])
                
                # Select canonical columns
                df = df.select(CANONICAL_SCHEMA.names())
                
                # --- PARALLEL INGESTION POINT ---
                try:
                    arrow_table = df.to_arrow()
                    # Push to Worker
                    self._push_to_worker(upload_queue, (arrow_table, "breach_records"), stop_event, error_container)
                except Exception as e:
                    log_error(f"Failed to ingest chunk {chunk_idx}: {e}")
                    q_path = staging_dir.parent / "quarantine" / f"failed_ingest_{input_path.stem}_{chunk_idx}.parquet"
                    df.write_parquet(q_path)
            
            # End of Stream
            if not stop_event.is_set():
                self._push_to_worker(upload_queue, None, stop_event, error_container)
                upload_queue.join()
                # worker_thread.join() # Daemon threads don't strictly need join if we ensured queue is empty, but good practice.
            
            if error_container:
                raise error_container[0]

            log_success(f"Ingestão Paralela via Streaming concluída para {input_path}. Total chunks: {chunk_idx}")
            
        except KeyboardInterrupt:
            log_warning("Ingestão interrompida pelo usuário.")
            stop_event.set()
            raise
        except Exception as e:
            log_error(f"Erro durante streaming raw de {input_path}: {e}")
            self._move_to_quarantine(input_path, staging_dir.parent / "quarantine")
            stop_event.set()

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
