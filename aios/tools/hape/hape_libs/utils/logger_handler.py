#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import logging.handlers
import os
import errno


def mkdir_p(path):
    try:
        os.makedirs(path, mode=0o755, exist_ok=True)  # Python>3.2
    except TypeError:
        try:
            os.makedirs(path)
        except OSError as exc:  # Python >2.5
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise


class CustomFileHandler(logging.handlers.RotatingFileHandler):
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0, encoding=None, delay=0):
        mkdir_p(os.path.dirname(filename))
        super(CustomFileHandler, self).__init__(filename, mode, maxBytes, backupCount, encoding, delay)
