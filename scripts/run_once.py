from utils.create_postgres_table import CreatePostgresTable

from clean_csv_data.validate_csv import CsvValidator
from clean_csv_data.clean_duplicates import DuplicatesRemover

from move_data.csv_to_datalake import CsvToPostgresLoader
from move_data.move_data_to_warehouse import MoveCleanDataToWarehouse


def run_once(config, table_config):

    """ clean the csv with pydantic """
    validate_csv = CsvValidator(config)
    clean_duplicates = DuplicatesRemover(config)
    validate_csv.validate_csv()
    clean_duplicates.clean_duplicates()

    """ create a postgresql datalake table """
    table = CreatePostgresTable(config, config['Tables']['datalake_table'], table_config['DatalakeColumns'])
    table.create_table()

    """ create a postgresql data warehouse table """
    table = CreatePostgresTable(config, config['Tables']['warehouse_table'], table_config['DataWarehouseColumns'])
    table.create_table()

    """ upload modified csv data to postgresql """
    loader = CsvToPostgresLoader(config)
    loader.copy_data_to_postgres()
    loader.commit_and_close()

    """ move csv data to data warehouse table """
    data_warehouse = MoveCleanDataToWarehouse(config, table_config)
    data_warehouse.move_data()
    data_warehouse.close_connection()
