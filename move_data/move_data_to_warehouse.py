from psycopg2 import sql
from typing import Dict, Any

from utils.logger import get_logger
from utils.postgres_connector import PostgresConnector


class MoveCleanDataToWarehouse:
    def __init__(self, config: Dict[str, Any], column_config: Dict[str, Any]) -> None:
        """ initializing MoveCleanDataToWarehouse class for moving clean data from datalake to warehouse. """

        self.postgres = PostgresConnector(config)
        self.cursor = self.postgres.get_cursor()

        self.source_table: str = config['Tables']['datalake_table']
        self.destination_table: str = config['Tables']['warehouse_table']

        self.column_config: Dict[str, Any] = column_config['DataWarehouseColumns']
        self.logger = get_logger()

    def move_data(self) -> None:
        """ move clean data from datalake to warehouse.
            uses an INSERT INTO SELECT query with conflict resolution on web_url. """

        columns = [sql.Identifier(column) for column in self.column_config.keys()]

        move_data_query = sql.SQL("""
            INSERT INTO {} ({})
            SELECT {}
            FROM {}
            ON CONFLICT (web_url) DO NOTHING;
        """).format(
            sql.Identifier(self.destination_table),
            sql.SQL(', ').join(columns),
            sql.SQL(', ').join(columns),
            sql.Identifier(self.source_table)
        )

        try:
            self.cursor.execute(move_data_query)
            self.cursor.connection.commit()
            self.logger.info("data moved to warehouse table")
        except Exception as error:
            self.postgres.rollback_changes()
            self.logger.error(f"error during move_data: {error}")

    def close_connection(self) -> None:
        """ close database connection. """
        self.postgres.close_connection()
