import logging
import sys
import os
import unittest

def test_main():
    log_formatter = logging.Formatter('%(asctime)s %(levelname)s [%(module)s:%(funcName)s:%(lineno)d] %(message)s')
    stdout_handler = logging.StreamHandler(sys.stderr)
    stdout_handler.setFormatter(log_formatter)
    logger = logging.root
    logger.setLevel(logging.DEBUG)
    # if logger.hasHandlers():
        # logger.handlers = []
    logger.addHandler(stdout_handler)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.info('cwd: %s', os.getcwd())
    logging.info('cmd: PYTHONPATH=%s python2 %s', os.environ['PYTHONPATH'], ' '.join(sys.argv[:]))
    unittest.main()