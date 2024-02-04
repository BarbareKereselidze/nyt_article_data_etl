from psycopg2 import sql
from typing import Dict, Any

from utils.logger import get_logger
from utils.postgres_connector import PostgresConnector


class CsvToPostgresLoader:
    def __init__(self, config: Dict[str, Any]) -> None:
        """ initializing CsvToPostgresLoader class for loading csv data into postgresql. """
        self.postgres = PostgresConnector(config)
        self.cursor = self.postgres.get_cursor()

        self.csv_file_path: str = config['Paths']['clean_csv_path']
        self.table_name: str = config['Tables']['datalake_table']

        self.logger = get_logger()

    def copy_data_to_postgres(self) -> None:
        """ copy data from the cleaned csv file to the postgresql table.
            uses a copy query for efficient bulk data loading. """

        copy_data_query = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(
            sql.Identifier(self.table_name)
        )

        with open(self.csv_file_path, 'r') as f:
            self.cursor.copy_expert(copy_data_query, f)

    def commit_and_close(self) -> None:
        """ commit changes and close the database connection. """

        self.postgres.close_connection()
        self.logger.info(f" data copied from {self.csv_file_path} to {self.table_name}")
