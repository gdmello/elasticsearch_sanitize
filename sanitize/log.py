import logging

LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(LOG_FORMAT)


def get_logger(name, level):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger


def add_console_handler(logger):
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.DEBUG)
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)


def add_file_handler(logger, file_path):
    file_handler = logging.handlers.RotatingFileHandler(file_path,
                                                        maxBytes=0.5 * 10 ** 8,
                                                        backupCount=100)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
