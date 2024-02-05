import logging


def get_logger() -> logging.Logger:
    """ configure the logging module to display messages with info level and above.
        return the logger instance. """

    logging.basicConfig(level=logging.INFO)

    # set the level for apscheduler logger to ERROR
    logging.getLogger('apscheduler').setLevel(logging.ERROR)

    logger = logging.getLogger(__name__)

    return logger
