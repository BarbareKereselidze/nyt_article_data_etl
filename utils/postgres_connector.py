import psycopg2

from utils.logger import get_logger


class PostgresConnector:
    def __init__(self, config: dict) -> None:

        self.db_name = config['Postgres']['dbname']
        self.user = config['Postgres']['user']
        self.password = config['Postgres']['password']
        self.host = config['Postgres']['host']
        self.port = config['Postgres']['port']

        self.connection = psycopg2.connect(
            dbname=self.db_name,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )
        self.cursor = None

        self.logger = get_logger()

    def get_cursor(self):
        self.cursor = self.connection.cursor()
        return self.cursor

    def close_connection(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.commit()
                self.connection.close()
        except Exception as error:
            self.logger.error(f"the following error occurred, when closing the connection: {error}")
