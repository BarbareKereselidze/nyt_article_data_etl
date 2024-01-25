import pandas as pd
from psycopg2 import sql

from utils.postgres_connector import PostgresConnector
from utils.logger import get_logger


class CreateDatalake:
    def __init__(self, config):
        self.postgres = PostgresConnector(config)
        self.cursor = self.postgres.get_cursor()

        self.csv_file_path = config['Paths']['clean_csv_path']
        self.file_path = config['Paths']['csv_file_path']
        self.table_name = config['Postgres']['table_name']

        self.logger = get_logger()

    def create_table(self):
        df = pd.read_csv(self.csv_file_path, parse_dates=['pub_date'], nrows=100)

        pg_data_types = {
            'object': 'text',
            'float64': 'float8',
            'datetime64[ns, UTC]': 'timestamptz',
        }

        columns = [f"{column} {pg_data_types[str(df[column].dtype)]}" for column in df.columns]

        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(self.table_name),
            sql.SQL(', ').join(map(sql.SQL, columns))
        )

        self.cursor.execute(create_table_query)
        self.postgres.close_connection()

        return self.logger.info("datalake table created successfully")
        # return df.info()
