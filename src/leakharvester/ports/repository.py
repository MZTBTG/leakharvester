from typing import Protocol
import pyarrow as pa

class BreachRepository(Protocol):
    def insert_arrow_batch(self, table: pa.Table, table_name: str) -> None:
        """
        Inserts a PyArrow Table into the specified ClickHouse table.
        """
        ...
    
    def execute_ddl(self, ddl_statement: str) -> None:
        """
        Executes a DDL statement (CREATE TABLE, etc).
        """
        ...

    def create_staging_table(self, staging_table: str, source_table: str) -> None:
        """
        Creates a staging table with the same structure as the source table.
        """
        ...

    def drop_table(self, table_name: str) -> None:
        """
        Drops a table if it exists.
        """
        ...

    def replace_partition(self, target_table: str, staging_table: str, partition_id: str) -> None:
        """
        Replaces a partition in the target table with data from the staging table.
        """
        ...

    def get_columns(self, table_name: str) -> list[str]:
        """
        Returns a list of column names for the specified table.
        """
        ...

    def add_column(self, table_name: str, column_name: str, column_type: str = None) -> None:
        """
        Adds a new column to the table.
        """
        ...

    def get_arrow_stream_process(self, table_name: str, columns: list[str] = None):
        """
        Returns a subprocess.Popen object for streaming Arrow data to ClickHouse.
        """
        ...
