import clickhouse_connect
from clickhouse_connect.driver.client import Client
import pyarrow as pa
from leakharvester.ports.repository import BreachRepository
from leakharvester.config import settings

class ClickHouseAdapter(BreachRepository):
    def __init__(self) -> None:
        self.client: Client = clickhouse_connect.get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DB
        )

    def insert_arrow_batch(self, table: pa.Table, table_name: str) -> None:
        # ClickHouse Connect handles PyArrow tables directly
        self.client.insert_arrow(
            table=table_name,
            arrow_table=table
        )

    def execute_ddl(self, ddl_statement: str) -> None:
        self.client.command(ddl_statement)

    def close(self) -> None:
        self.client.close()
