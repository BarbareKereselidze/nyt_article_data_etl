import time
import json
import hashlib
import requests
from psycopg2 import IntegrityError

from helper.get_columns import get_table_columns
from utils.postgres_connector import PostgresConnector
from utils.logger import get_logger


class APIDataToPostgres:
    def __init__(self, config):
        self.postgres = PostgresConnector(config)
        self.connection = self.postgres.connection
        self.cursor = self.postgres.get_cursor()

        self.columns = get_table_columns(config)
        # self.table_name = 'test'
        self.table_name = config['Postgres']['table_name']

        self.api_key = config['API']['api_key']
        self.url = config['API']['api_url']
        self.params = {'api-key': self.api_key}

        self.logger = get_logger()

    def generate_hash(self, web_url):
        if web_url is not None:
            return hashlib.sha256(web_url.encode('utf-8')).hexdigest()
        return None

    def insert_data(self):
        while True:
            response = requests.get(self.url, params=self.params)

            if response.status_code == 200:
                data = response.json()

                for record in data['response']['docs']:
                    web_url = record.get('web_url')
                    if web_url is None:
                        continue
                    hash_value = self.generate_hash(web_url)

                    values = [hash_value]

                    for column in self.columns[1:]:
                        if column in record:
                            if isinstance(record[column], dict):
                                values.append(json.dumps(record[column]))
                            elif isinstance(record[column], list):
                                values.append(json.dumps(record[column]))
                            else:
                                values.append(record[column])
                        else:
                            values.append(None)

                    sql = f"INSERT INTO {self.table_name} ({', '.join(self.columns)}) VALUES ({', '.join(['%s']*len(values))})"

                    try:
                        self.cursor.execute(sql, values)
                        self.connection.commit()
                        self.logger.info("a")
                    except IntegrityError as e:
                        self.connection.rollback()
                        self.logger.warning(f"IntegrityError: {e}")

            else:
                self.logger.info(f"Error: {response.status_code} - {response.text}")

            time.sleep(10)

