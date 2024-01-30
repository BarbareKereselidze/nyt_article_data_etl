import json
import requests
from psycopg2 import IntegrityError
from datetime import datetime, timedelta

from helper.get_columns import get_table_columns
from utils.postgres_connector import PostgresConnector
from utils.logger import get_logger


class APIDataToPostgres:
    def __init__(self, config):
        self.postgres = PostgresConnector(config)
        self.connection = self.postgres.connection
        self.cursor = self.postgres.get_cursor()

        self.columns = get_table_columns(config)
        self.table_name = config['Postgres']['table_name']

        self.api_key = config['API']['api_key']
        self.url = config['API']['api_url']
        self.params = {'api-key': self.api_key}

        self.logger = get_logger()

    def insert_data(self):
        yesterday = datetime.now() - timedelta(days=1)
        begin_date = yesterday.strftime('%Y%m%d')
        end_date = datetime.now().strftime('%Y%m%d')

        params = {'api-key': self.api_key, 'begin_date': begin_date, 'end_date': end_date}
        response = requests.get(self.url, params=params)

        if response.status_code == 200:
            data = response.json()

            for record in data['response']['docs']:
                web_url = record.get('web_url')
                if not web_url:
                    continue
                values = [json.dumps(record.get(column)) if isinstance(record.get(column), (dict, list)) else record.get(column, None) for column in self.columns]

                sql = f"INSERT INTO {self.table_name} ({', '.join(self.columns)}) VALUES ({', '.join(['%s']*len(values))})"

                try:
                    self.cursor.execute(sql, values)
                    self.connection.commit()
                    self.logger.info("Data inserted successfully.")
                except IntegrityError as e:
                    self.connection.rollback()
                    self.logger.warning(f"IntegrityError: {e}")

            self.logger.info("Finished processing today's data.")

        else:
            self.logger.info(f"Error: {response.status_code} - {response.text}")
