import threading
from typing import TYPE_CHECKING, Optional
from leakharvester.ports.repository import BreachRepository
from leakharvester.config import settings
from leakharvester.settings_manager import SettingsManager

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client
    import pyarrow as pa

class ClickHouseAdapter(BreachRepository):
    def __init__(self) -> None:
        self._thread_local = threading.local()
        self.host = settings.CLICKHOUSE_HOST
        self.port = settings.CLICKHOUSE_PORT
        self.username = settings.CLICKHOUSE_USER
        self.password = settings.CLICKHOUSE_PASSWORD
        self.database = settings.CLICKHOUSE_DB
        self.settings = {
            "async_insert": 1, 
            "wait_for_async_insert": 0,
            "max_partitions_per_insert_block": 1000,
            "allow_experimental_inverted_index": 1
        }

    @property
    def client(self) -> "Client":
        if not hasattr(self._thread_local, 'client'):
            import clickhouse_connect
            self._thread_local.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                settings=self.settings,
                connect_timeout=30,
                send_receive_timeout=600
            )
        return self._thread_local.client

    def insert_arrow_batch(self, table: "pa.Table", table_name: str) -> None:
        self.client.insert_arrow(
            table=table_name,
            arrow_table=table
        )

    def execute_ddl(self, ddl_statement: str) -> None:
        self.client.command(ddl_statement)

    def create_staging_table(self, staging_table: str, source_table: str) -> None:
        """Creates a staging table with the same structure as the source table."""
        self.client.command(f"CREATE TABLE IF NOT EXISTS {staging_table} AS {source_table}")

    def drop_table(self, table_name: str) -> None:
        """Drops a table if it exists."""
        self.client.command(f"DROP TABLE IF EXISTS {table_name}")

    def replace_partition(self, target_table: str, staging_table: str, partition_id: str) -> None:
        """Replaces a partition in the target table with data from the staging table."""
        self.client.command(f"ALTER TABLE {target_table} REPLACE PARTITION '{partition_id}' FROM {staging_table}")

    def get_columns(self, table_name: str) -> list[str]:
        """Returns a list of column names for the specified table."""
        db, table = table_name.split('.') if '.' in table_name else (self.database, table_name)
        result = self.client.query(f"SELECT name FROM system.columns WHERE database = '{db}' AND table = '{table}'")
        return [row[0] for row in result.result_rows]

    def get_columns_with_types(self, table_name: str) -> list[tuple[str, str]]:
        """Returns a list of (name, type) tuples for the specified table."""
        db, table = table_name.split('.') if '.' in table_name else (self.database, table_name)
        result = self.client.query(f"SELECT name, type FROM system.columns WHERE database = '{db}' AND table = '{table}'")
        return [(row[0], row[1]) for row in result.result_rows]

    def add_column(self, table_name: str, column_name: str, column_type: Optional[str] = None) -> None:
        """Adds a new column to the table."""
        if column_type is None:
            compression_level = SettingsManager().get_compression_level()
            column_type = f"String CODEC(ZSTD({compression_level}))"
        self.client.command(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")

    def get_table_stats(self, table_name: str) -> dict:
        """Fetches storage and row statistics for the table."""
        db, table = table_name.split('.') if '.' in table_name else (self.database, table_name)
        sql = f"""
        SELECT 
            sum(rows) as total_rows,
            formatReadableSize(sum(data_compressed_bytes)) as compressed_size,
            formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
            round(sum(data_uncompressed_bytes) / nullIf(sum(data_compressed_bytes),0), 2) as compression_ratio
        FROM system.parts
        WHERE database = '{db}' AND table = '{table}' AND active = 1
        """
        result = self.client.query(sql).result_rows
        if not result or result[0][0] is None:
             return {"total_rows": 0, "compressed_size": "0 B", "uncompressed_size": "0 B", "compression_ratio": 0.0}
        
        return {
            "total_rows": result[0][0],
            "compressed_size": result[0][1],
            "uncompressed_size": result[0][2],
            "compression_ratio": result[0][3]
        }

    def get_arrow_stream_process(self, table_name: str, columns: list[str] = None):
        """Returns a subprocess.Popen object for streaming Arrow data to ClickHouse."""
        import subprocess
        import shutil

        if not shutil.which("clickhouse-client"):
             raise RuntimeError("The 'clickhouse-client' binary is not found in PATH. Please install ClickHouse client tools.")

        native_port = 9000
        
        query = f"INSERT INTO {table_name} FORMAT ArrowStream"
        if columns:
            col_str = ", ".join(columns)
            query = f"INSERT INTO {table_name} ({col_str}) FORMAT ArrowStream"
        
        # Fix: Enable async_insert to prevent 'Too many parts' backpressure during high-speed ingestion
        query += " SETTINGS async_insert=1, wait_for_async_insert=0"
        
        cmd = [
            "clickhouse-client",
            "--host", self.host,
            "--port", str(native_port),
            "--user", self.username,
            "--password", self.password,
            "--query", query
        ]
        
        return subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

    def get_source_file_stats(self, table_name: str, limit: int = 200) -> list:
        """Returns aggregated stats per source file."""
        sql = f"""
        SELECT 
            source_file, 
            count() as row_count, 
            min(import_date) as first_seen, 
            max(import_date) as last_seen
        FROM {table_name}
        GROUP BY source_file 
        ORDER BY row_count DESC 
        LIMIT {limit}
        """
        return self.client.query(sql).result_rows

    def get_indices(self, table_name: str) -> list:
        """Returns list of skipping indices with size."""
        db, table = table_name.split('.') if '.' in table_name else (self.database, table_name)
        sql = f"""
        SELECT 
            name, 
            type, 
            expr, 
            granularity,
            formatReadableSize(data_compressed_bytes) as size
        FROM system.data_skipping_indices 
        WHERE database = '{db}' AND table = '{table}'
        """
        return self.client.query(sql).result_rows

    def get_partitions(self, table_name: str) -> list[str]:
        """Returns list of active partition IDs for the table."""
        db, table = table_name.split('.') if '.' in table_name else (self.database, table_name)
        sql = f"SELECT distinct partition_id FROM system.parts WHERE database = '{db}' AND table = '{table}' AND active = 1"
        return [row[0] for row in self.client.query(sql).result_rows]

    def stream_query(self, query: str):
        """Streams query results row by row."""
        with self.client.query_rows_stream(query) as stream:
            yield from stream

    def stream_raw_query(self, query: str, fmt: str = 'CSVWithNames'):
        """Streams raw bytes directly from ClickHouse with specified format."""
        with self.client.raw_stream(query, fmt=fmt) as stream:
            yield from stream

    def close(self) -> None:
        if hasattr(self._thread_local, 'client'):
            self._thread_local.client.close()
            del self._thread_local.client

    def reset_connection(self) -> None:
        """Forces the thread-local client to reconnect on next access."""
        self.close()
