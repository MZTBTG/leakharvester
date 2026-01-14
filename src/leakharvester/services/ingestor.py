from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, TYPE_CHECKING
import threading
import queue
import uuid
import csv
import io
import re
from rich.prompt import Confirm
from rich.console import Console
from rich.panel import Panel
from leakharvester.ports.repository import BreachRepository
from leakharvester.ports.file_storage import FileStorage
from leakharvester.domain.rules import detect_column_mapping
from leakharvester.domain.schemas import CANONICAL_SCHEMA
from leakharvester.adapters.console import log_info, log_error, log_warning, log_success

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

class BreachIngestor:
    def __init__(self, repository: BreachRepository, file_storage: FileStorage):
        self.repository = repository
        self.file_storage = file_storage

    def _parse_format_string(self, format_str: str) -> Tuple[str, List[str]]:
        """
        Parses the format string to determine delimiter and column names.
        Example: 'email:pass:doc' -> (':', ['email', 'password', 'document'])
        """
        if format_str == "auto" or format_str == "email:pass":
             return ":", ["email", "password"]

        delimiters = [":", ",", ";", "|"]
        detected_delimiter = ":" 
        
        counts = {d: format_str.count(d) for d in delimiters}
        best_delimiter = max(counts, key=counts.get)
        
        if counts[best_delimiter] > 0:
            detected_delimiter = best_delimiter
            
        columns = [c.strip() for c in format_str.split(detected_delimiter)]
        
        normalized_cols = []
        for c in columns:
            if c == "pass": normalized_cols.append("password")
            elif c == "user": normalized_cols.append("username")
            else: normalized_cols.append(c)
            
        return detected_delimiter, normalized_cols

    def _validate_and_sync_schema(self, columns: List[str]) -> bool:
        """
        Checks if columns exist in ClickHouse. Prompts user to create missing ones.
        """
        try:
            existing_cols = set(self.repository.get_columns("vault.breach_records"))
        except Exception as e:
            log_error(f"Failed to fetch schema from DB: {e}")
            return False

        missing_cols = []
        for col in columns:
            if col == "null" or col == "unknown": continue
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

    def _finalize_partition_swap(self, target_table: str, staging_table: str, partition_id: str) -> None:
        try:
            log_info(f"Swapping partition '{partition_id}' from {staging_table} to {target_table}...")
            self.repository.replace_partition(target_table, staging_table, partition_id)
            log_success(f"Partition swap successful for {partition_id}.")
        except Exception as e:
            log_error(f"Partition swap failed: {e}")
            raise e
        finally:
            log_info(f"Dropping staging table {staging_table}...")
            self.repository.drop_table(staging_table)

    def _detect_encoding(self, path: Path) -> str:
        """
        Detects file encoding by trying to read the first block.
        Prioritizes UTF-8, falls back to Latin-1.
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                f.read(4096)
            return "utf-8"
        except UnicodeDecodeError:
            return "latin-1"

    def _analyze_and_suggest_format(self, input_path: Path) -> Dict[str, Any]:
        """
        Robustly detects CSV format using python's csv.Sniffer and encoding checks.
        Returns a configuration dictionary for Polars parser.
        """
        config = {
            "separator": ",",
            "quote_char": '"',
            "has_header": False,
            "encoding": "utf-8",
            "columns": []
        }

        try:
            encoding = self._detect_encoding(input_path)
            config["encoding"] = encoding

            sample = ""
            with open(input_path, "r", encoding=encoding, newline='') as f:
                for _ in range(10):
                    line = f.readline()
                    if not line: break
                    sample += line

            if not sample: 
                return config

            sniffer = csv.Sniffer()
            try:
                dialect = sniffer.sniff(sample, delimiters=[',', ';', ':', '|', '\t'])
                config["separator"] = dialect.delimiter
                config["quote_char"] = dialect.quotechar
                config["has_header"] = sniffer.has_header(sample)
            except csv.Error:
                if ":" in sample: config["separator"] = ":"
                elif ";" in sample: config["separator"] = ";"

            try:
                sample_io = io.StringIO(sample)
                import polars as pl
                df = pl.read_csv(
                    sample_io, 
                    separator=config["separator"],
                    quote_char=config["quote_char"],
                    has_header=config["has_header"],
                    n_rows=10,
                    ignore_errors=True
                )
                
                col_types = []
                email_regex = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
                
                for col_name in df.columns:
                    col_data = df[col_name].cast(pl.String)
                    
                    if config["has_header"]:
                        c_lower = col_name.lower()
                        if "email" in c_lower or "mail" in c_lower:
                            col_types.append("email")
                            continue
                        if "pass" in c_lower or "pwd" in c_lower:
                            col_types.append("password")
                            continue
                    
                    match_count = 0
                    valid_rows = 0
                    for val in col_data:
                        if not val: continue
                        valid_rows += 1
                        if email_regex.search(val):
                            match_count += 1
                    
                    if valid_rows > 0 and (match_count / valid_rows) > 0.5:
                        col_types.append("email")
                    else:
                        col_types.append("unknown")
                
                if "email" in col_types and "password" not in col_types:
                    if len(col_types) == 2:
                        idx = col_types.index("email")
                        col_types[1-idx] = "password"
                
                config["columns"] = col_types

            except Exception:
                pass

            return config

        except Exception as e:
            log_warning(f"Format detection failed: {e}")
            return config

    def process_file(
        self, 
        input_path: Path, 
        staging_dir: Path, 
        quarantine_dir: Path,
        batch_size: int = 500_000,
        format: str = "auto",
        no_check: bool = False,
        custom_source_name: Optional[str] = None,
        num_workers: int = 1,
        append: bool = False
    ) -> None:
        log_info(f"Iniciando processamento de: {input_path} [Format: {format}, NoCheck: {no_check}, Workers: {num_workers}, Append: {append}]")
        
        target_table = "vault.breach_records"
        source_label = custom_source_name or input_path.name
        staging_table = None

        if append:
            ingest_table = target_table
        else:
            staging_table = f"vault.staging_{uuid.uuid4().hex}"
            log_info(f"Creating staging table: {staging_table}")
            self.repository.create_staging_table(staging_table, target_table)
            ingest_table = staging_table

        # Initialize concurrency primitives early to satisfy finally block
        upload_queue = queue.Queue(maxsize=num_workers * 2)
        stop_event = threading.Event()
        workers = []

        try:
            # 1. Configuration
            config = {}
            if format != "auto":
                delimiter, cols = self._parse_format_string(format)
                config = {
                    "separator": delimiter,
                    "quote_char": '"',
                    "has_header": False,
                    "encoding": self._detect_encoding(input_path),
                    "columns": cols
                }
            else:
                config = self._analyze_and_suggest_format(input_path)
                
                # Check for ambiguity
                cols = config.get("columns", [])
                log_info(f"Detected Config: {config}")
                has_essentials = "email" in cols and "password" in cols
                
                # We proceed if we found essential cols, even if there are extras (which will be ignored/dropped if unknown)
                # We only abort if we lack essentials AND have ambiguity (unknowns or complex structure)
                is_ambiguous = not cols or ("unknown" in cols and not has_essentials) or (len(cols) > 2 and not has_essentials)

                if is_ambiguous:
                    # Ambiguous - Suggest and Abort
                    console = Console()
                    fmt_hint = config.get("separator", ":").join(cols)
                    
                    msg = f"""
[bold yellow]Ambiguous File Structure Detected[/bold yellow]
Auto-ingestion paused to prevent data corruption.

[bold]Detected Config:[/bold] Sep='{config.get("separator")}' Header={config.get("has_header")}
[bold]Mapped Columns:[/bold] {cols}

[bold green]Suggested Command:[/bold green]
leakharvester ingest --file "{input_path}" --format "{fmt_hint}"

[dim]Replace 'unknown' with standard names (username, ip, etc) or 'null'.[/dim]
                    """
                    console.print(Panel(msg, title="Format Suggestion", border_style="yellow"))
                    
                    if staging_table and not append: self.repository.drop_table(staging_table)
                    return

            if "columns" in config:
                if not self._validate_and_sync_schema(config["columns"]):
                    if staging_table and not append: self.repository.drop_table(staging_table)
                    return

            # 2. Setup Workers
            error_container = []

            for _ in range(num_workers):
                t = threading.Thread(
                    target=self._ingestion_worker, 
                    args=(upload_queue, stop_event, error_container),
                    daemon=True
                )
                t.start()
                workers.append(t)

            # 3. Native Parsing Loop
            chunk_idx = 0
            
            try:
                import polars as pl
                reader = pl.read_csv_batched(
                    input_path,
                    separator=config.get("separator", ","),
                    quote_char=config.get("quote_char", '"'),
                    has_header=config.get("has_header", False),
                    batch_size=batch_size,
                                            ignore_errors=True,
                                            truncate_ragged_lines=True,
                                            low_memory=True,
                                            encoding="utf8-lossy", # Always use lossy for robustness
                                            infer_schema_length=1000
                                        )
                while True:
                    if stop_event.is_set():
                        break
                    
                    batches = reader.next_batches(1)
                    if not batches:
                        break
                    
                    df = batches[0]
                    chunk_idx += 1
                    
                    # 4. Column Mapping
                    detected_cols = config.get("columns", [])
                    current_cols = df.columns
                    rename_ops = {}
                    
                    if not config.get("has_header", False) and detected_cols:
                        for i, target_name in enumerate(detected_cols):
                            if i < len(current_cols) and target_name != "unknown":
                                rename_ops[current_cols[i]] = target_name
                    elif config.get("has_header", False):
                        rename_ops = detect_column_mapping(current_cols)

                    if rename_ops:
                        df = df.rename(rename_ops)

                    # 5. Validation
                    if "email" in df.columns:
                        df = df.filter(pl.col("email").is_not_null() & (pl.col("email") != ""))
                        
                        if not no_check:
                            validation_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
                            bad_df = df.filter(~pl.col("email").str.contains(validation_pattern))
                            
                            if bad_df.height > 0:
                                q_path = quarantine_dir / f"quarantine_{source_label}_{chunk_idx}.parquet"
                                try:
                                    bad_df.write_parquet(q_path, compression="zstd")
                                except Exception: pass
                            
                            df = df.filter(pl.col("email").str.contains(validation_pattern))

                    if df.height == 0:
                        continue

                    # 6. Metadata
                    df = df.with_columns([
                        pl.lit(source_label).alias("source_file"),
                        pl.lit(None).cast(pl.Date).alias("breach_date"),
                        pl.lit(None).cast(pl.Datetime).alias("import_date")
                    ])

                    # 7. Schema Alignment
                    target_cols = CANONICAL_SCHEMA.names()
                    current_set = set(df.columns)
                    missing_exprs = []
                    
                    for tc in target_cols:
                        if tc not in current_set:
                            if tc == "breach_date" or tc == "import_date": continue
                            missing_exprs.append(pl.lit("").alias(tc))
                    
                    if missing_exprs:
                        df = df.with_columns(missing_exprs)

                    cols_to_keep = [c for c in CANONICAL_SCHEMA.names() if c in df.columns]
                    final_df = df.select(cols_to_keep)
                    
                    exprs = [pl.col(name).cast(dtype) for name, dtype in CANONICAL_SCHEMA.items() if name in final_df.columns]
                    final_df = final_df.select(exprs)

                    try:
                        arrow_table = final_df.to_arrow()
                        self._push_to_worker(upload_queue, (arrow_table, ingest_table), stop_event, error_container)
                    except Exception as e:
                        log_error(f"Failed to ingest chunk {chunk_idx}: {e}")
                        q_path = staging_dir.parent / "quarantine" / f"failed_ingest_{input_path.stem}_{chunk_idx}.parquet"
                        final_df.write_parquet(q_path)

                if not stop_event.is_set():
                    for _ in range(num_workers):
                        self._push_to_worker(upload_queue, None, stop_event, error_container)
                    upload_queue.join()
                    for t in workers:
                        t.join()

                if error_container:
                    raise error_container[0]

                log_success(f"Ingestão concluída. Total chunks: {chunk_idx}")
                
                if not append:
                    self._finalize_partition_swap(target_table, staging_table, source_label)

            except Exception as e:
                log_error(f"Processing error: {e}")
                raise e

        except KeyboardInterrupt:
            log_warning("Ingestão interrompida pelo usuário.")
            stop_event.set()
            if staging_table: self.repository.drop_table(staging_table)
            raise
        except Exception as e:
            log_error(f"Erro durante processamento de {input_path}: {e}")
            self._move_to_quarantine(input_path, quarantine_dir)
            stop_event.set()
            if staging_table: self.repository.drop_table(staging_table)
        finally:
            for t in workers:
                t.join(timeout=2.0)

    def process_stream(self, stream: Any, staging_dir: Path, quarantine_dir: Path, batch_size: int, source_name: str = "stdin", format: str = "auto", no_check: bool = False, num_workers: int = 1, append: bool = False) -> None:
        """
        Ingests data from a stream (stdin/pipe) using the dynamic logic.
        """
        import warnings
        import time
        warnings.filterwarnings("ignore", message="CSV malformed")
        
        target_table = "vault.breach_records"
        staging_table = None
        
        if append:
            ingest_table = target_table
        else:
            staging_table = f"vault.staging_{uuid.uuid4().hex}"
            log_info(f"Creating staging table: {staging_table}")
            self.repository.create_staging_table(staging_table, target_table)
            ingest_table = staging_table

        chunk_idx = 0
        total_rows = 0
        start_time = time.time()
        
        # Stream ingestion requires simpler logic as we can't sniff effectively easily
        # Fallback to defaults or provided format
        delimiter, columns = self._parse_format_string(format)
        if not self._validate_and_sync_schema(columns):
            if staging_table: self.repository.drop_table(staging_table)
            return

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
        
        log_info(f"Iniciando ingestão via Stream ({source_name}) [Format: {format}] [Delim: '{delimiter}'] [Cols: {columns}] [Append: {append}]")
        
        try:
            import polars as pl
            for df in self.file_storage.read_stream_batched(stream, batch_size=batch_size):
                if stop_event.is_set():
                    break  

                chunk_idx += 1
                total_rows += df.height
                
                if chunk_idx % 10 == 0:
                     elapsed = time.time() - start_time
                     log_info(f"Processando chunk {chunk_idx} ({total_rows} linhas)... {elapsed:.2f}s")
                
                num_cols = len(columns)
                df = df.with_columns(
                    pl.col("raw_line").str.splitn(delimiter, num_cols).alias("split_parts")
                )
                
                exprs = []
                for i, col_name in enumerate(columns):
                    if col_name == "null":
                        continue
                    exprs.append(
                        pl.col("split_parts").struct.field(f"field_{i}").alias(col_name)
                    )
                
                df = df.with_columns(exprs)
                
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

                df = df.with_columns([
                    pl.lit(source_name).alias("source_file"),
                    pl.lit(None).cast(pl.Date).alias("breach_date"),
                    pl.lit(None).cast(pl.Datetime).alias("import_date")
                ])
                
                target_cols = CANONICAL_SCHEMA.names()
                current_cols = set(df.columns)
                missing_exprs = []
                for tc in target_cols:
                    if tc not in current_cols:
                         if tc == "breach_date" or tc == "import_date": continue 
                         missing_exprs.append(pl.lit("").alias(tc))
                
                if missing_exprs:
                    df = df.with_columns(missing_exprs)
                
                cols_to_keep = list(set(CANONICAL_SCHEMA.names()) | (set(columns) - {"null"}))
                final_df = df.select([c for c in cols_to_keep if c in df.columns])
                
                try:
                    arrow_table = final_df.to_arrow()
                    self._push_to_worker(upload_queue, (arrow_table, ingest_table), stop_event, error_container)
                except Exception as e:
                    log_error(f"Failed to convert chunk {chunk_idx}: {e}")
            
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
            
            if not append:
                self._finalize_partition_swap(target_table, staging_table, source_name)

        except KeyboardInterrupt:
            log_warning("Ingestão interrompida pelo usuário.")
            stop_event.set()
            if staging_table: self.repository.drop_table(staging_table)
            raise
        except Exception as e:
            log_error(f"Erro durante ingestão de stream: {e}")
            stop_event.set()
            if staging_table: self.repository.drop_table(staging_table)
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

                if item is None:
                    q.task_done()
                    break
                
                try:
                    table, table_name = item
                    self.repository.insert_arrow_batch(table, table_name)
                except Exception as e:
                    log_error(f"Worker Upload Failed: {e}")
                    if not error_container:
                        error_container.append(e)
                    error_event.set()
                finally:
                    q.task_done()
                
                if error_event.is_set():
                    try:
                        while True:
                            q.get_nowait()
                            q.task_done()
                    except queue.Empty:
                        pass
                    break

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
        upload_queue = queue.Queue(maxsize=3)
        stop_event = threading.Event()
        error_container = []

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
                    import pyarrow as pa
                    table = pa.Table.from_batches([batch])
                    self._push_to_worker(upload_queue, (table, "breach_records"), stop_event, error_container)
                except Exception as e:
                    log_error(f"Failed to process parquet batch: {e}")
                    raise e
            
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
                import polars as pl
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
                    valid_df = valid_df.with_columns([
                        pl.lit(q_file.name).alias("source_file"),
                        pl.lit("").alias("username"),
                        pl.lit("").alias("password"),
                        pl.lit(None).cast(pl.Date).alias("breach_date"),
                        pl.lit(None).cast(pl.Datetime).alias("import_date")
                    ])
                    
                    valid_df = valid_df.select(CANONICAL_SCHEMA.names())
                    
                    table = valid_df.to_arrow()
                    self.repository.insert_arrow_batch(table, "breach_records")
                    
                    total_recovered += valid_df.height
                    log_info(f"Recuperado {valid_df.height} linhas de {q_file.name}")
                
                q_file.unlink()
                
            except Exception as e:
                log_error(f"Erro ao reparar {q_file}: {e}")
        
        log_success(f"Reparo concluído. Total recuperado: {total_recovered} registros.")
