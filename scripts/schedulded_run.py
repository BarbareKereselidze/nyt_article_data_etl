from get_api_data.api_data_to_table import DataInserter

from helper.get_last_date import get_last_date


def scheduled_run(config, table_config):
    """ get the last date from the datalake table """
    last_date = get_last_date(config)

    """ extract configuration for the datalake and data warehouse tables """
    datalake_table = config["Tables"]["datalake_table"]
    datalake_columns = table_config['DatalakeColumns']

    data_warehouse_table = config["Tables"]["warehouse_table"]
    data_warehouse_columns = table_config['DataWarehouseColumns']

    """ add new api data to the datalake and data warehouse """
    api_data_inserter = DataInserter(config, last_date, {
        datalake_table: datalake_columns,
        data_warehouse_table: data_warehouse_columns
    })

    api_data_inserter.process_data()
