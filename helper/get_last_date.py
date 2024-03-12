import psycopg2
from datetime import datetime
from typing import Dict, Any, Optional

from utils.postgres_connector import PostgresConnector
from utils.logger import get_logger


def get_last_date(config: Dict[str, Any]) -> Optional[datetime]:
    """ get the last date from the specified postgresql table."""

    postgres_connector = PostgresConnector(config)
    table_name: str = config['Tables']['datalake_table']
    logger = get_logger()

    try:
        cursor = postgres_connector.get_cursor()

        # select the max date from pub_date column
        cursor.execute(f"SELECT MAX(DATE(pub_date)) FROM {table_name};")
        last_date = cursor.fetchone()[0]

        return last_date

    except psycopg2.Error as error:
        logger.error(f"error getting last date: {error}")

    finally:
        postgres_connector.close_connection()
