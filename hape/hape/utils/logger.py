import os
import sys
import inspect
import logging
import logging.config
import logging.handlers
path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(path)
from logger_handler import *

class Logger(object):
    logger_name = None

    @staticmethod
    def init(logging_conf_path):
        logging.config.fileConfig(logging_conf_path)

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
    def access_log(*msg):
        logger = logging.getLogger('access')
        logger.debug(*msg)

    @staticmethod
    def _get_logger():
        frm = inspect.stack()[2]
        if Logger.logger_name:
            name = Logger.logger_name
        else:
            name = inspect.getmodule(frm[0]).__name__
        return logging.getLogger(name)

    @staticmethod
    def _log_prefix():
        _, modulepath, line, fun_name, _, _ = inspect.stack()[2]
        _, tail = os.path.split(modulepath)
        log_str = '[' + tail + ':' + str(fun_name) + ':' + str(line) + ']: '
        return log_str
    

    @staticmethod
    def init_stdout_logger(is_debug):
        if is_debug:
            logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s")
        else:
            logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] [%(threadName)s] %(message)s")
    
if __name__ == "__main__":
    Logger.init_stdout_logger(False)
    Logger.warning("test")
    