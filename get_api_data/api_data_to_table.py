import json
import time
from psycopg2 import IntegrityError
from typing import Dict, List

from get_api_data.get_nyt_api import APIDataRetriever

from utils.logger import get_logger
from utils.postgres_connector import PostgresConnector


class DataInserter:
    def __init__(self, config: Dict[str, str], begin_date: str, table_column_mapping: Dict[str, List[str]]) -> None:
        """ initialize the DataInserter class. """

        self.postgres = PostgresConnector(config)
        self.connection = self.postgres.connection
        self.cursor = self.postgres.get_cursor()

        self.api_data = APIDataRetriever(config, begin_date)

        # getting table column mapping to be able to insert data uin two tables at once
        self.table_column_mapping: Dict[str, List[str]] = table_column_mapping
        self.logger = get_logger()

    def __del__(self):
        """ destructor to ensure connection closure when the object is deleted. """

        if hasattr(self, 'connection') and self.connection:
            self.postgres.close_connection()
            self.logger.info(" database connection closed.")

    def insert_data(self, record: dict) -> None:
        """ check if the data is valid and insert it into the according table. """

        web_url: str = record.get('web_url')

        # checking if the web_url is valid, since it's supposed to be the unique column
        if not web_url:
            return

        for table_name, columns in self.table_column_mapping.items():
            # returning json type data if it's a list or dict, so it will have an appropriate type jsonb
            values = [json.dumps(record.get(column)) if isinstance(record.get(column), (dict, list)) else
                      record.get(column, None) for column in columns]

            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"

            try:
                self.cursor.execute(sql, values)
                self.connection.commit()
                """ detailed logging (can be uncommented) """
                # self.logger.info(f" inserted into {table_name}: {web_url}")

            # in case of duplicate data rollback connection
            except IntegrityError:
                """ detailed logging (can be uncommented) """
                # self.logger.info(f" duplicate data in {table_name}: {web_url}")
                self.connection.rollback()

            # sleeping for the recommended time for this api
            time.sleep(12)

    def process_data(self) -> None:
        """ process api data, insert it into the according tables and """
        page: int = 0

        while True:
            status_code, data = self.api_data.get_api_data(page)

            if status_code is not None and data is not None:
                # if there is a connection insert data into the database
                if status_code == 200:
                    docs = data['response']['docs']

                    # break the loop when there are no more documents
                    if not docs:
                        break

                    for record in docs:
                        self.insert_data(record)

                    page += 1

                else:
                    self.logger.info(f" error: {status_code} - {data.text}")
                    break

            else:
                break

        self.logger.info(" finished processing new data.")
