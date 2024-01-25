from psycopg2 import sql

from utils.logger import get_logger
from utils.postgres_connector import PostgresConnector


class CsvToPostgresLoader:
    def __init__(self, config: dict) -> None:
        self.postgres = PostgresConnector(config)
        self.cursor = self.postgres.get_cursor()

        self.csv_file_path = config['Paths']['clean_csv_path']
        self.table_name = config['Postgres']['table_name']

        self.logger = get_logger()

    def copy_data_to_postgres(self):

        copy_data_query = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(
            sql.Identifier(self.table_name)
        )

        with open(self.csv_file_path, 'r') as f:
            self.cursor.copy_expert(copy_data_query, f)

    def commit_and_close(self):
        self.postgres.close_connection()
        return self.logger.info(f"data copied from {self.csv_file_path} to {self.table_name}")
