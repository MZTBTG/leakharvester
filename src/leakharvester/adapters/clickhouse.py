import clickhouse_connect
from clickhouse_connect.driver.client import Client
import pyarrow as pa
import threading
from leakharvester.ports.repository import BreachRepository
from leakharvester.config import settings

class ClickHouseAdapter(BreachRepository):
    def __init__(self) -> None:
        self._thread_local = threading.local()
        # We also keep connection params to re-create clients
        self.host = settings.CLICKHOUSE_HOST
        self.port = settings.CLICKHOUSE_PORT
        self.username = settings.CLICKHOUSE_USER
        self.password = settings.CLICKHOUSE_PASSWORD
        self.database = settings.CLICKHOUSE_DB
        self.settings = {"async_insert": 1, "wait_for_async_insert": 0}

    @property
    def client(self) -> Client:
        if not hasattr(self._thread_local, 'client'):
            self._thread_local.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                database=self.database,
                settings=self.settings
            )
        return self._thread_local.client

    def insert_arrow_batch(self, table: pa.Table, table_name: str) -> None:
        # ClickHouse Connect handles PyArrow tables directly
        self.client.insert_arrow(
            table=table_name,
            arrow_table=table
        )

    def execute_ddl(self, ddl_statement: str) -> None:
        self.client.command(ddl_statement)

    def get_columns(self, table_name: str) -> list[str]:
        """Returns a list of column names for the specified table."""
        db, table = table_name.split('.') if '.' in table_name else (self.database, table_name)
        result = self.client.query(f"SELECT name FROM system.columns WHERE database = '{db}' AND table = '{table}'")
        return [row[0] for row in result.result_rows]

    def add_column(self, table_name: str, column_name: str, column_type: str = "String CODEC(ZSTD(3))") -> None:
        """Adds a new column to the table."""
        self.client.command(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {column_name} {column_type}")

    def close(self) -> None:
        # Best effort close for current thread
        if hasattr(self._thread_local, 'client'):
            self._thread_local.client.close()
            del self._thread_local.client
