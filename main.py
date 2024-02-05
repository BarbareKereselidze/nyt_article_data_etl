import os
from apscheduler.schedulers.blocking import BlockingScheduler

from utils.read_config import read_config_file
from utils.logger import get_logger

from scripts.run_once import run_once
from scripts.schedulded_run import scheduled_run


if __name__ == "__main__":
    """ create a logger instance """
    logger = get_logger()

    """ get config file paths """
    script_directory = os.path.dirname(os.path.abspath(__file__))
    CONFIG_FILE_PATH = os.path.join(script_directory, 'config', 'config.ini')
    TABLE_CONFIG_PATH = os.path.join(script_directory, 'config', 'table_config.ini')

    """ get file path dicts from a config file """
    config_dict = read_config_file(CONFIG_FILE_PATH)
    table_config_dict = read_config_file(TABLE_CONFIG_PATH)

    """ run the script that doesn't need to be scheduled first """
    run_once(config_dict, table_config_dict)

    """ schedule the script that will be run everyday at 12am """
    scheduler = BlockingScheduler()
    scheduler.add_job(scheduled_run, 'cron', hour=0, minute=0, args=[config_dict, table_config_dict])

    try:
        logger.info(" scheduler is running")
        scheduler.start()
    except KeyboardInterrupt:
        logger.info(" scheduler stopped")
