from typing import Protocol, Any
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
