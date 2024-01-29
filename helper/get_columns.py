from utils.postgres_connector import PostgresConnector


def get_table_columns(config):
    postgres = PostgresConnector(config)
    cursor = postgres.get_cursor()
    table_name = config['Postgres']['table_name']

    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position")
    columns = [column[0] for column in cursor.fetchall()]

    postgres.close_connection()

    return columns
