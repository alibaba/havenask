#!/bin/env python
import os
import time

def log_info(msg):
    print '%s INFO %s' % (time.strftime('%Y-%m-%d %H:%M:%S'), msg)
    
def log_error(msg):
    print '%s ERROR %s' % (time.strftime('%Y-%m-%d %H:%M:%S'), msg)
    
def log_warn(msg):
    print '%s WARN %s' % (time.strftime('%Y-%m-%d %H:%M:%S'), msg)
