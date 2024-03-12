import psycopg2
from typing import Dict, Optional

from utils.logger import get_logger


class PostgresConnector:
    def __init__(self, config: Dict[str, str]) -> None:
        """ initialize the PostgresConnector class. """

        self.db_name: str = config['Postgres']['dbname']
        self.user: str = config['Postgres']['user']
        self.password: str = config['Postgres']['password']
        self.host: str = config['Postgres']['host']
        self.port: int = config['Postgres']['port']

        self.connection = psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor: Optional[psycopg2.extensions.cursor] = None

        self.logger = get_logger()

    def get_cursor(self) -> psycopg2.extensions.cursor:
        """ get postgresql cursor. """

        self.cursor = self.connection.cursor()
        return self.cursor

    def rollback_changes(self) -> None:
        """ rollback changes made in the current transaction. """

        self.connection.rollback()

    def close_connection(self) -> None:
        """ close the postgresql connection.
            commits changes if there are any and then closes the connection. """

        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.commit()
                self.connection.close()
        except Exception as error:
            self.logger.error(f"the following error occurred, when closing the connection: {error}")
