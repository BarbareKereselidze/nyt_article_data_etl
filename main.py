import os

from utils.read_config import read_config_file

from upload_csv_to_postgres.clean_data import process_csv
from upload_csv_to_postgres.create_datalake import CreateDatalake
from upload_csv_to_postgres.csv_to_postgres import CsvToPostgresLoader

from helper.get_columns import get_table_columns
from get_api_data.get_nyt_api import APIDataToPostgres


if __name__ == "__main__":

    """ get config file path """
    script_directory = os.path.dirname(os.path.abspath(__file__))
    CONFIG_FILE_PATH = os.path.join(script_directory, 'config', 'config.ini')

    """ get file paths dict from a config file """
    config_dict = read_config_file(CONFIG_FILE_PATH)

    """ clean the csv with pydantic """
    # process_csv(config_dict)

    """ create a postgresql table datalake """
    table = CreateDatalake(config_dict)
    table.create_table()

    """ upload modified csv data to postgresql """
    # loader = CsvToPostgresLoader(config_dict)
    # loader.copy_data_to_postgres()
    # loader.commit_and_close()

    """ get new api data """
    columns = get_table_columns(config_dict)
    api_loader = APIDataToPostgres(config_dict)
    api_loader.insert_data()

    """ create a new table for cleaned data """
