from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List, TYPE_CHECKING, Callable
import threading
import queue
import uuid
import csv
import io
import re
import cchardet
from leakharvester.config import settings
from leakharvester.ports.repository import BreachRepository
from leakharvester.ports.file_storage import FileStorage
from leakharvester.domain.schemas import CANONICAL_SCHEMA
from leakharvester.domain.exceptions import SchemaMismatchError, AmbiguousFormatException
from leakharvester.adapters.console import log_info, log_error, log_warning, log_success

import polars as pl
import pyarrow as pa


class BreachIngestor:
    def __init__(self, repository: BreachRepository, file_storage: FileStorage):
        self.repository = repository
        self.file_storage = file_storage

    def _parse_format_string(self, format_str: str) -> Tuple[str, List[str]]:
        """
        Parses the format string to determine delimiter and column names.
        Example: 'email:pass:doc' -> (':', ['email', 'pass', 'doc'])
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
        
        # Removed magic normalization to respect user input
        return detected_delimiter, columns

    def _map_polars_type_to_clickhouse(self, pl_type: Any) -> str:
        import polars as pl
        if pl_type == pl.Date: return "Date"
        if pl_type == pl.Datetime: return "DateTime"
        if pl_type in (pl.Int8, pl.Int16, pl.Int32, pl.Int64): return "Nullable(Int64)"
        if pl_type in (pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64): return "Nullable(UInt64)"
        if pl_type in (pl.Float32, pl.Float64): return "Nullable(Float64)"
        return "String"

    def _validate_and_sync_schema(self, columns_schema: Dict[str, str], on_schema_mismatch: Optional[Callable[[List[str]], bool]] = None) -> bool:
        """
        Checks if columns exist in ClickHouse. Uses callback to confirm addition of missing ones.
        columns_schema: Dict mapping column name to ClickHouse type (e.g., {"age": "Nullable(Int64)"}).
        """
        try:
            existing_cols = set(self.repository.get_columns(settings.BREACH_TABLE))
        except Exception as e:
            log_error(f"Failed to fetch schema from DB: {e}")
            return False

        missing_cols = []
        for col in columns_schema.keys():
            if col == "null" or col == "unknown": continue
            if col not in existing_cols:
                missing_cols.append(col)
        
        if not missing_cols:
            return True

        log_warning(f"The following columns are missing in the database: {missing_cols}")
        
        should_add = False
        if on_schema_mismatch:
            should_add = on_schema_mismatch(missing_cols)
        
        if should_add:
            try:
                for col in missing_cols:
                    col_type = columns_schema.get(col, "String")
                    log_info(f"Adding column '{col}' type '{col_type}'...")
                    self.repository.add_column(settings.BREACH_TABLE, col, col_type)
                log_success("Schema updated successfully.")
                return True
            except Exception as e:
                log_error(f"Failed to update schema: {e}")
                return False
        else:
            log_error("Ingestion aborted due to schema mismatch.")
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
        Detects file encoding using cchardet.
        """
        try:
            with open(path, "rb") as f:
                raw = f.read(32768)
                if not raw: return "utf-8"
                result = cchardet.detect(raw)
                return result["encoding"] or "utf-8"
        except Exception as e:
            log_warning(f"Encoding detection failed, defaulting to utf-8: {e}")
            return "utf-8"

    def _detect_separator_competition(self, sample_bytes: bytes, encoding: str) -> Tuple[str, Any]:
        """
        Competes multiple separators against the sample to find the best fit using Polars.
        Returns (best_separator, sample_dataframe).
        """
        candidates = [",", ";", "|", ":", "\t"]
        best_sep = ","
        best_score = -1.0
        best_df = None
        
        import polars as pl

        for sep in candidates:
            try:
                df = pl.read_csv(
                    io.BytesIO(sample_bytes),
                    separator=sep,
                    has_header=False,
                    n_rows=50,
                    ignore_errors=True,
                    encoding=encoding,
                    truncate_ragged_lines=True,
                    infer_schema_length=0
                )
                
                if df.width <= 1: 
                    continue

                null_counts_row = df.null_count().row(0)
                null_count = sum(null_counts_row)
                total_cells = df.height * df.width
                if total_cells == 0: continue
                
                null_ratio = null_count / total_cells
                
                score = df.width * (1.0 - null_ratio)
                
                if score > best_score:
                    best_score = score
                    best_sep = sep
                    best_df = df
            except Exception:
                continue
                
        return best_sep, best_df

    def _analyze_and_suggest_format(self, input_path: Path) -> Dict[str, Any]:
        """
        Robustly detects CSV format using "Competition" strategy.
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

            with open(input_path, "rb") as f:
                sample_bytes = f.read(65536)

            if not sample_bytes: 
                return config

            separator, best_df = self._detect_separator_competition(sample_bytes, encoding)
            
            if separator:
                config["separator"] = separator
            
            if best_df is None:
                return config

            try:
                first_row = best_df.row(0)
                first_row_str = [str(v).lower() for v in first_row if v is not None]
                
                header_keywords = {"email", "mail", "e-mail", "password", "pass", "pwd", "user", "username", "login", "ip", "url", "site"}
                intersection = set(first_row_str) & header_keywords
                
                if intersection:
                    config["has_header"] = True
                
                # If header is present, we need to get the real column names.
                # We need to re-parse with has_header=True to get them from the parser
                import polars as pl
                if config["has_header"]:
                    df_header = pl.read_csv(
                        io.BytesIO(sample_bytes),
                        separator=config["separator"],
                        has_header=True,
                        n_rows=10,
                        ignore_errors=True,
                        encoding=encoding,
                        truncate_ragged_lines=True
                    )
                    config["columns"] = df_header.columns
                else:
                    # No header, try to identify content columns
                    # We use best_df (which has generic headers column_1, etc)
                    # We infer based on content regex
                    col_types = []
                    email_regex = re.compile(r"[^@\s]+@[^@\s]+\.[^@\s]+")
                    
                    for col_idx in range(best_df.width):
                        col_data = best_df.select(pl.col(best_df.columns[col_idx]).cast(pl.String)).to_series()
                        
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
                    
                    # Heuristic for password: if 2 cols and one is email
                    if "email" in col_types and "password" not in col_types:
                        if len(col_types) == 2:
                            idx = col_types.index("email")
                            col_types[1-idx] = "password"
                    
                    config["columns"] = col_types

            except Exception as e:
                log_warning(f"Header analysis failed: {e}")

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
        skip_email_validation: bool = False,
        custom_source_name: Optional[str] = None,
        num_workers: int = 1,
        append: bool = False,
        on_schema_mismatch: Optional[Callable[[List[str]], bool]] = None
    ) -> None:
        log_info(f"Starting processing of: {input_path} [Format: {format}, SkipVal: {skip_email_validation}, Workers: {num_workers}, Append: {append}]")
        
        target_table = settings.BREACH_TABLE
        source_label = custom_source_name or input_path.name
        staging_table = None

        if append:
            ingest_table = target_table
        else:
            staging_table = f"vault.staging_{uuid.uuid4().hex}"
            log_info(f"Creating staging table: {staging_table}")
            self.repository.create_staging_table(staging_table, target_table)
            ingest_table = staging_table

        stop_event = threading.Event()

        try:
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
                
                cols = config.get("columns", [])
                log_info(f"Detected Config: {config}")
                
                # Check for ambiguity
                # Apply mapping rules to see if we found essentials (e.g. 'E-Mail' -> 'email')
                has_essentials = False
                if config.get("has_header", False):
                    mapping = detect_column_mapping(cols)
                    mapped_values = list(mapping.values())
                    has_essentials = "email" in mapped_values and "password" in mapped_values
                else:
                    has_essentials = "email" in cols and "password" in cols
                
                is_ambiguous = not cols or ("unknown" in cols and not has_essentials) or (len(cols) > 2 and not has_essentials)

                if is_ambiguous:
                    if staging_table and not append: self.repository.drop_table(staging_table)
                    raise AmbiguousFormatException(config, cols, str(input_path))

            # Setup Worker State
            procs = []
            write_queues = []
            writer_threads = []
            writer_error = []
            
            def _stream_writer(idx, process_handle, q):
                arrow_writer = None
                try:
                    while True:
                        item = q.get()
                        if item is None:
                            q.task_done()
                            break
                        
                        table = item
                        try:
                            if arrow_writer is None:
                                arrow_writer = pa.ipc.new_stream(process_handle.stdin, table.schema)
                            arrow_writer.write_table(table)
                        except Exception as e:
                            writer_error.append(e)
                            break
                        finally:
                            q.task_done()
                            
                except Exception as e:
                    writer_error.append(e)
                finally:
                    try:
                        if arrow_writer: 
                            arrow_writer.close()
                    except (OSError, ValueError): pass 
                    try: 
                        if process_handle.stdin: 
                            process_handle.stdin.close()
                    except (OSError, ValueError): pass

            try:

                
                chunk_idx = 0
                reader = pl.read_csv_batched(
                    input_path,
                    separator=config.get("separator", ","),
                    quote_char=config.get("quote_char", '"'),
                    has_header=config.get("has_header", False),
                    batch_size=batch_size,
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                    encoding=config.get("encoding", "utf8"),
                    infer_schema_length=1000
                )
                
                # Read first batch to infer schema
                batches = reader.next_batches(1)
                if not batches:
                    log_warning("File is empty or no data found.")
                    if staging_table and not append: self.repository.drop_table(staging_table)
                    return

                first_batch = batches[0]
                
                # Dynamic Schema Inference & Sync
                schema_map = {}
                detected_cols = config.get("columns", [])
                current_cols = first_batch.columns
                
                if config.get("has_header", False):
                    # Trust header names but apply canonical mapping first
                    mapping = {} # detect_column_mapping(current_cols)
                    
                    for col_name in current_cols:
                        target_col = mapping.get(col_name, col_name)
                        schema_map[target_col] = self._map_polars_type_to_clickhouse(first_batch.schema[col_name])
                else:
                    # Map positional columns if defined in config
                    for i, target_name in enumerate(detected_cols):
                        if i < len(current_cols) and target_name != "unknown":
                            col_name_in_df = current_cols[i]
                            schema_map[target_name] = self._map_polars_type_to_clickhouse(first_batch.schema[col_name_in_df])

                if not self._validate_and_sync_schema(schema_map, on_schema_mismatch=on_schema_mismatch):
                    if staging_table and not append: self.repository.drop_table(staging_table)
                    return

                try:
                    valid_db_cols = set(self.repository.get_columns(target_table))
                except Exception as e:
                    log_error(f"Failed to fetch schema: {e}")
                    if staging_table and not append: self.repository.drop_table(staging_table)
                    return
                
                # Determine Potential Columns
                potential_cols = ["source_file"] + list(schema_map.keys())
                # Deduplicate
                seen = set()
                potential_cols = [x for x in potential_cols if not (x in seen or seen.add(x))]
                target_cols = [c for c in potential_cols if c in valid_db_cols]
                
                if not target_cols:
                    log_error("No valid columns found to ingest.")
                    if staging_table and not append: self.repository.drop_table(staging_table)
                    return

                log_info(f"Starting Native Arrow Stream to {ingest_table} with {num_workers} workers...")
                
                # Start Workers
                for i in range(num_workers):
                    p = self.repository.get_arrow_stream_process(ingest_table, columns=target_cols)
                    procs.append(p)
                    q = queue.Queue(maxsize=3)
                    write_queues.append(q)
                    t = threading.Thread(target=_stream_writer, args=(i, p, q), daemon=True)
                    t.start()
                    writer_threads.append(t)

                pending_batches = [first_batch]
                
                while True:
                    if writer_error:
                        break
                    
                    if not pending_batches:
                        pending_batches = reader.next_batches(1)
                        if not pending_batches:
                            break
                    
                    df = pending_batches.pop(0)
                    chunk_idx += 1
                    
                    # Rename Logic
                    detected_cols = config.get("columns", [])
                    current_cols = df.columns
                    rename_ops = {}
                    
                    if not config.get("has_header", False) and detected_cols:
                        for i, target_name in enumerate(detected_cols):
                            if i < len(current_cols) and target_name != "unknown":
                                rename_ops[current_cols[i]] = target_name
                    elif config.get("has_header", False):
                        rename_ops = {} # detect_column_mapping(current_cols)

                    if rename_ops:
                        df = df.rename(rename_ops)

                    if "email" in df.columns:
                        df = df.filter(pl.col("email").is_not_null() & (pl.col("email") != ""))
                        
                        if not skip_email_validation:
                            validation_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
                            bad_df = df.filter(~pl.col("email").str.contains(validation_pattern))
                            
                            if bad_df.height > 0:
                                q_path = quarantine_dir / f"quarantine_{source_label}_{chunk_idx}.parquet"
                                try:
                                    bad_df.write_parquet(q_path, compression="zstd")
                                except Exception as e:
                                    log_error(f"Failed to write to quarantine {q_path}: {e}")
                            
                            df = df.filter(pl.col("email").str.contains(validation_pattern))

                    if df.height == 0:
                        continue

                    df = df.with_columns([
                        pl.lit(source_label).alias("source_file"),
                    ])

                    current_set = set(df.columns)
                    
                    missing_exprs = []
                    for tc in target_cols:
                        if tc not in current_set:
                            # Use proper nulls for type
                            # If column exists in DB but not DF, fill with null/empty
                            # Check schema map for type? Defaults to string/null.
                            # Polars lit(None) defaults to null.
                            missing_exprs.append(pl.lit(None).alias(tc))
                    
                    if missing_exprs:
                        df = df.with_columns(missing_exprs)

                    final_df = df.select(target_cols)

                    try:
                        arrow_table = final_df.to_arrow()
                        worker_idx = (chunk_idx - 1) % num_workers
                        write_queues[worker_idx].put(arrow_table)
                        
                    except Exception as e:
                        log_error(f"Failed to ingest chunk {chunk_idx}: {e}")
                        q_path = quarantine_dir / f"failed_ingest_{input_path.stem}_{chunk_idx}.parquet"
                        final_df.write_parquet(q_path)

                # Cleanup
                for q in write_queues:
                    q.put(None)
                
                for t in writer_threads:
                    t.join()
                
                if writer_error:
                    raise writer_error[0]
                
                for p in procs:
                    p.stdin = None
                    stdout, stderr = p.communicate()
                    if p.returncode != 0:
                        raise Exception(f"Worker process failed: {stderr.decode()}")

                if not procs:
                    log_warning("No data processed.")

                log_success(f"Ingestion completed. Total chunks: {chunk_idx}")
                
                if not append:
                    self._finalize_partition_swap(target_table, staging_table, source_label)

            except Exception as e:
                log_error(f"Processing error: {e}")
                for p in procs:
                    try: p.kill()
                    except: pass
                raise e

        except KeyboardInterrupt:
            log_warning("Ingestion interrupted by user.")
            stop_event.set()
            if staging_table: self.repository.drop_table(staging_table)
            raise
        except Exception as e:
            log_error(f"Error during processing of {input_path}: {e}")
            stop_event.set()
            if staging_table: self.repository.drop_table(staging_table)
            self._move_to_quarantine(input_path, quarantine_dir)
        finally:
            pass

    def process_stream(self, stream: Any, staging_dir: Path, quarantine_dir: Path, batch_size: int, source_name: str = "stdin", format: str = "auto", skip_email_validation: bool = False, num_workers: int = 1, append: bool = False, on_schema_mismatch: Optional[Callable[[List[str]], bool]] = None) -> None:
        """
        Ingests data from a stream (stdin/pipe) using the dynamic logic.
        """
        import warnings
        import time
        warnings.filterwarnings("ignore", message="CSV malformed")
        
        target_table = settings.BREACH_TABLE
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
        
        delimiter, columns = self._parse_format_string(format)
        if not self._validate_and_sync_schema(columns, on_schema_mismatch=on_schema_mismatch):
            if staging_table: self.repository.drop_table(staging_table)
            return

        log_info(f"Starting ingestion via Stream ({source_name}) [Format: {format}] [Delim: '{delimiter}'] [Cols: {columns}] [Append: {append}]")
        log_info(f"Starting Native Arrow Stream to {ingest_table} with {num_workers} workers...")

        # State for multi-worker
        procs = []
        write_queues = []
        writer_threads = []
        writer_error = []
        
        def _stream_writer(idx, process_handle, q):
            arrow_writer = None
            try:
                while True:
                    item = q.get()
                    if item is None:
                        q.task_done()
                        break
                    
                    table = item
                    try:
                        if arrow_writer is None:
                            arrow_writer = pa.ipc.new_stream(process_handle.stdin, table.schema)
                        arrow_writer.write_table(table)
                    except Exception as e:
                        writer_error.append(e)
                        break
                    finally:
                        q.task_done()
                        
            except Exception as e:
                writer_error.append(e)
            finally:
                try:
                    if arrow_writer: 
                        arrow_writer.close()
                except (OSError, ValueError): pass 
                try: 
                    if process_handle.stdin: 
                        process_handle.stdin.close()
                except (OSError, ValueError): pass
        
        try:

            
            for df in self.file_storage.read_stream_batched(stream, batch_size=batch_size):
                if writer_error:
                    break

                chunk_idx += 1
                total_rows += df.height
                
                if chunk_idx % 10 == 0:
                     elapsed = time.time() - start_time
                     log_info(f"Processing chunk {chunk_idx} ({total_rows} lines)... {elapsed:.2f}s")
                
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
                     
                     if not skip_email_validation:
                        validation_pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
                        bad_df = df.filter(~pl.col("email").str.contains(validation_pattern))
                        
                        if bad_df.height > 0:
                             q_path = quarantine_dir / f"quarantine_{source_name}_{chunk_idx}.parquet"
                             try:
                                 bad_df.select(["raw_line"]).write_parquet(q_path, compression="zstd")
                             except Exception as e:
                                 log_error(f"Failed to write to quarantine {q_path}: {e}")
                        
                        df = df.filter(pl.col("email").str.contains(validation_pattern))

                if df.height == 0:
                    continue

                df = df.with_columns([
                    pl.lit(source_name).alias("source_file"),
                ])
                
                target_cols = ["source_file", "email", "username", "password"]
                current_set = set(df.columns)
                missing_exprs = []
                for tc in target_cols:
                    if tc not in current_set:
                         missing_exprs.append(pl.lit("").alias(tc))
                
                if missing_exprs:
                    df = df.with_columns(missing_exprs)
                
                final_df = df.select(target_cols)
                
                try:
                    arrow_table = final_df.to_arrow()
                    
                    # Initialize processes on first batch
                    if not procs:
                        for i in range(num_workers):
                            p = self.repository.get_arrow_stream_process(ingest_table, columns=target_cols)
                            procs.append(p)
                            q = queue.Queue(maxsize=3) # Small buffer per worker
                            write_queues.append(q)
                            t = threading.Thread(target=_stream_writer, args=(i, p, q), daemon=True)
                            t.start()
                            writer_threads.append(t)

                    # Round-robin distribution
                    worker_idx = (chunk_idx - 1) % num_workers
                    write_queues[worker_idx].put(arrow_table)

                except Exception as e:
                    log_error(f"Failed to convert chunk {chunk_idx}: {e}")

            # Cleanup
            for q in write_queues:
                q.put(None)
            
            for t in writer_threads:
                t.join()
            
            if writer_error:
                raise writer_error[0]
            
            for p in procs:
                p.stdin = None 
                stdout, stderr = p.communicate()
                if p.returncode != 0:
                    raise Exception(f"Worker process failed: {stderr.decode()}")

            if not procs:
                log_warning("No data processed.")
            
            total_time = time.time() - start_time
            log_success(f"Stream ingestion completed. Total chunks: {chunk_idx} | Lines: {total_rows} | Time: {total_time:.2f}s")
            
            if not append:
                self._finalize_partition_swap(target_table, staging_table, source_name)

        except KeyboardInterrupt:
            log_warning("Ingestion interrupted by user.")
            stop_event.set()
            if staging_table: self.repository.drop_table(staging_table)
            raise
        except Exception as e:
            log_error(f"Error during stream ingestion: {e}")
            stop_event.set()
            if staging_table: self.repository.drop_table(staging_table)
            # Kill procs
            for p in procs:
                try: p.kill()
                except: pass
        finally:
            pass

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
                    table = pa.Table.from_batches([batch])
                    self._push_to_worker(upload_queue, (table, settings.BREACH_TABLE), stop_event, error_container)
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
            log_warning(f"File moved to quarantine: {dest}")
        except Exception as e:
            log_error(f"Failed to move to quarantine: {e}")

    def repair_quarantine(self, quarantine_dir: Path, staging_dir: Path) -> None:
        """
        Scans quarantine directory, tries to extract emails using heavy regex, and ingests recovered data.
        """
        files = list(quarantine_dir.glob("*.parquet"))
        if not files:
            log_info("No files found in quarantine.")
            return
            
        log_info(f"Starting repair of {len(files)} files in quarantine...")
        
        email_pattern = r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
        total_recovered = 0
        
        for q_file in files:
            try:
                import polars as pl
                df = pl.read_parquet(q_file)
                if df.height == 0:
                    q_file.unlink()
                    continue
                
                df = df.with_columns(
                    pl.col("raw_line").str.extract(email_pattern, 1).alias("email")
                )
                
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
                    self.repository.insert_arrow_batch(table, settings.BREACH_TABLE)
                    
                    total_recovered += valid_df.height
                    log_info(f"Recovered {valid_df.height} lines from {q_file.name}")
                
                q_file.unlink()
                
            except Exception as e:
                log_error(f"Error repairing {q_file}: {e}")
        
        log_success(f"Repair completed. Total recovered: {total_recovered} records.")
