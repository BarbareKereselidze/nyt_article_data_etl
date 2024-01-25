import logging


def get_logger() -> logging.Logger:

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    return logger
