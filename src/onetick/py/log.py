import os
import sys
import json
import logging
import warnings
from pathlib import Path
from .configuration import config


class CustomStreamHandler(logging.StreamHandler):
    # workaround for https://github.com/pytest-dev/pytest/issues/5502
    # pytest doesn't work well when you initialize logging at import time
    # and then try logging to stdout at application exit
    # (we do it when deleting temporary files)
    # in this case logging system writes very cumbersome error messages,
    # so here we override them
    def handleError(self, record):
        if logging.raiseExceptions and sys.stderr:
            try:
                msg = self.format(record)
                sys.stderr.write(msg + self.terminator)
                sys.stderr.flush()
            except Exception:
                pass


def get_stream_handler():
    handler = CustomStreamHandler()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s (%(name)s): %(message)s')
    handler.setFormatter(formatter)
    return handler


ROOT_LOGGER_NAME = 'onetick.py'
ROOT_LOGGER = None


def __init_root_logger():
    global ROOT_LOGGER

    if ROOT_LOGGER is not None:
        return

    ROOT_LOGGER = logging.getLogger(ROOT_LOGGER_NAME)
    ROOT_LOGGER.addHandler(logging.NullHandler())

    if not config.logging:
        return

    if os.sep in config.logging:
        config_file = Path(config.logging)
        if not config_file.exists():
            warnings.warn(f"Configuration file '{config_file}' doesn't exist, can't initialize logging system")
            return

        # Config file is crafted and loaded by user, so we can ignore SonarQube warnings
        if config_file.suffix == '.json':
            with config_file.open() as f:
                json_data = json.load(f)
            logging.config.dictConfig(json_data)  # NOSONAR
        else:
            logging.config.fileConfig(config_file)  # NOSONAR
    else:
        try:
            ROOT_LOGGER.setLevel(config.logging)
        except ValueError:
            warnings.warn(f"Unknown logging level name: '{config.logging}', can't initialize logging system")
            return
        ROOT_LOGGER.addHandler(get_stream_handler())

    ROOT_LOGGER.debug(f'Initialized {ROOT_LOGGER_NAME} logging system')


__init_root_logger()


def get_logger(*parts):
    name = '.'.join(parts)
    if not name.startswith(ROOT_LOGGER_NAME):
        raise ValueError(f"Logger name '{name}' must start with root logger name '{ROOT_LOGGER_NAME}'")
    logger = logging.getLogger(name)
    return logger


def get_debug_logger():
    logger = logging.getLogger('onetick.py.otq_debug_mode')
    logger.setLevel("DEBUG" if config.otq_debug_mode else "INFO")
    return logger
