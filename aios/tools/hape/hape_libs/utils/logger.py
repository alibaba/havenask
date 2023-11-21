import os
import sys
import inspect
import logging
import logging.config
import logging.handlers
path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(path)
from logger_handler import *

class Logger(object):
    logger_name = None

    @staticmethod
    def debug(*msg):
        log = Logger._get_logger()
        prefix = Logger._log_prefix() + msg[0]
        prefix_msg = (prefix, ) + msg[1:]
        log.debug(*prefix_msg)

    @staticmethod
    def log(lvl, *msg):
        log = Logger._get_logger()
        prefix = Logger._log_prefix() + msg[0]
        prefix_msg = (prefix, ) + msg[1:]
        log.log(lvl, *prefix_msg)

    @staticmethod
    def info(*msg):
        log = Logger._get_logger()
        prefix = Logger._log_prefix() + msg[0]
        prefix_msg = (prefix, ) + msg[1:]
        log.info(*prefix_msg)

    @staticmethod
    def warning(*msg):
        log = Logger._get_logger()
        prefix = Logger._log_prefix() + msg[0]
        prefix_msg = (prefix, ) + msg[1:]
        log.warning(*prefix_msg)

    @staticmethod
    def error(*msg):
        log = Logger._get_logger()
        prefix = Logger._log_prefix() + msg[0]
        prefix_msg = (prefix, ) + msg[1:]
        log.error(*prefix_msg)

    @staticmethod
    def _get_logger():
        return logging.getLogger("hape")

    @staticmethod
    def _log_prefix():
        _, modulepath, line, fun_name, _, _ = inspect.stack()[2]
        _, tail = os.path.split(modulepath)
        log_str = '[' + tail + ':' + str(fun_name) + ':' + str(line) + ']: '
        return log_str

    @staticmethod
    def init_stdout_logger(level):
        logger = logging.getLogger("hape")
        logger.propagate = False
        if not logger.handlers:
            formatter = logging.Formatter(fmt='[%(asctime)s] [%(levelname)s] %(message)s')
            handler = logging.StreamHandler()
            logger.setLevel(level)
            handler.setLevel(level)
            handler.setFormatter(formatter)
            logger.addHandler(handler)

    
if __name__ == "__main__":
    Logger.init_stdout_logger(False)
    Logger.warning("test warn")
    Logger.info("test info")
    