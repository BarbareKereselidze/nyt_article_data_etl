import logging


def get_logger() -> logging.Logger:
    """ configure the logging module to display messages with info level and above.
        return the logger instance. """

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    return logger
