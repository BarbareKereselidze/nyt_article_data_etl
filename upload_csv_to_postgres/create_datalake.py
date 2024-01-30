from psycopg2 import sql

from utils.postgres_connector import PostgresConnector
from utils.logger import get_logger


class CreateDatalake:
    def __init__(self, config):
        self.postgres = PostgresConnector(config)
        self.cursor = self.postgres.get_cursor()

        self.table_name = config['Postgres']['table_name']

        self.logger = get_logger()

    def create_table(self):
        columns = [
            # "id serial PRIMARY KEY",
            "web_url text UNIQUE NOT NULL",
            "abstract text",
            "snippet text",
            "lead_paragraph text",
            "print_section text",
            "print_page float8",
            "source text",
            "multimedia jsonb",
            "headline jsonb",
            "keywords jsonb",
            "pub_date timestamptz",
            "document_type text",
            "news_desk text",
            "section_name text",
            "byline jsonb",
            "type_of_material text",
            "_id text",
            "word_count float8",
            "uri text",
            "subsection_name text",
        ]

        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
            sql.Identifier(self.table_name),
            sql.SQL(', ').join(map(sql.SQL, columns))
        )

        self.cursor.execute(create_table_query)
        self.postgres.close_connection()

        return self.logger.info("datalake table created successfully")
