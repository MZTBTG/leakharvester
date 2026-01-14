import threading
from typing import TYPE_CHECKING, List, Tuple
from leakharvester.ports.repository import BreachRepository
from leakharvester.config import settings

if TYPE_CHECKING:
    import clickhouse_connect
    from clickhouse_connect.driver.client import Client
    import pyarrow as pa

class ClickHouseAdapter(BreachRepository):
    def __init__(self) -> None:
        self._thread_local = threading.local()
        # We also keep connection params to re-create clients
        self.host = settings.CLICKHOUSE_HOST
        self.port = settings.CLICKHOUSE_PORT
        self.username = settings.CLICKHOUSE_USER
        self.password = settings.CLICKHOUSE_PASSWORD
        self.database = settings.CLICKHOUSE_DB
        self.settings = {
            "async_insert": 1, 
            "wait_for_async_insert": 0,
            "max_partitions_per_insert_block": 1000
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
                settings=self.settings
            )
        return self._thread_local.client

    def insert_arrow_batch(self, table: "pa.Table", table_name: str) -> None:
        # ClickHouse Connect handles PyArrow tables directly
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
        # For LowCardinality(String), the partition ID needs to be properly quoted in the SQL
        # If partition_id is 'filename.txt', it should be treated as a string literal.
        # ClickHouse syntax: REPLACE PARTITION 'partition_name' ...
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

    def add_column(self, table_name: str, column_name: str, column_type: str = "String CODEC(ZSTD(3))") -> None:
        """Adds a new column to the table."""
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

    def get_source_file_stats(self, table_name: str, limit: int = 50) -> list:
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
        """Returns list of skipping indices."""
        db, table = table_name.split('.') if '.' in table_name else (self.database, table_name)
        sql = f"SELECT name, type, expr, granularity FROM system.data_skipping_indices WHERE database = '{db}' AND table = '{table}'"
        return self.client.query(sql).result_rows

    def get_partitions(self, table_name: str) -> list[str]:
        """Returns list of active partition IDs for the table."""
        db, table = table_name.split('.') if '.' in table_name else (self.database, table_name)
        sql = f"SELECT distinct partition_id FROM system.parts WHERE database = '{db}' AND table = '{table}' AND active = 1"
        return [row[0] for row in self.client.query(sql).result_rows]

    def close(self) -> None:
        # Best effort close for current thread
        if hasattr(self._thread_local, 'client'):
            self._thread_local.client.close()
            del self._thread_local.client
