from psycopg2 import sql
from typing import Dict, Any, List

from utils.postgres_connector import PostgresConnector
from utils.logger import get_logger


class CreatePostgresTable:
    def __init__(self, config: Dict[str, Any], table_name: str, columns: Dict[str, str]) -> None:
        """ initialize the CreatePostgresTable class. """
        self.postgres = PostgresConnector(config)
        self.cursor = self.postgres.get_cursor()

        self.table_name: str = table_name
        self.columns: List[str] = [f"{column} {definition}" for column, definition in columns.items()]

        self.logger = get_logger()

    def create_table(self) -> None:
        """ create a postgresql table if it does not already exist. """

        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(self.table_name),
            sql.SQL(', ').join(map(sql.SQL, self.columns))
        )

        self.cursor.execute(create_table_query)
        self.postgres.close_connection()

        self.logger.info(f"{self.table_name} table created successfully")
