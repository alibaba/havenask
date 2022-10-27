#!/bin/env python
# -*- coding: utf-8 -*-

import subprocess
import os
import sys
import json

def run(cmd):
    p = subprocess.Popen(cmd, shell=True, close_fds=False,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    return stdout, stderr, p.returncode

data, error ,code = run("find %s -name '*_table.json'" % sys.argv[1])

fileList = filter(None, data.split("\n"))

def loadJsonFile(fileName):
    if not os.path.exists(fileName):
        return {}
    f = open(fileName, 'r')
    ret = json.read(f.read())
    f.close()
    return ret

for file in fileList:
    print file
    jsonData = loadJsonFile(file)
    if 'raw_data_swift_config' in jsonData:
        del jsonData['raw_data_swift_config']
    if 'reader_config' in jsonData:
        del jsonData['reader_config']
    f = open(file, 'w')
    f.write(json.write(jsonData, True))
    f.close()

