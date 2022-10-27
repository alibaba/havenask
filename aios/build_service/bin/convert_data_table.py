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

data, error ,code = run("find %s -name 'data_source.json' -or -name 'processor.json'" % sys.argv[1])

fileList=data.split("\n")
uniqDir = set()
for f in fileList:
    uniqDir.add(f[0:f.rfind('/')])

print uniqDir

def loadJsonFile(fileName):
    if not os.path.exists(fileName):
        return {}
    f = open(fileName, 'r')
    ret = json.read(f.read())
    f.close()
    os.remove(fileName)
    return ret

for config in uniqDir:
    if len(config) == 0:
        continue
    json1 = loadJsonFile(config + '/processor.json')
    json2 = loadJsonFile(config + '/data_source.json')
    os.system('mkdir -p %s/data_tables' % config)
    f = open(config + '/data_tables/simple_table.json', 'w')
    if not json1:
        print config + '/processor.json' + ' not exist'
        continue
    if not json2:
        json2 = {
            "raw_data_swift_config" : {
                "topic" : {
                    "partition_buffer_size" : 1,
                    "partition_resource" : 1,
                }
            },
            "reader_config" : {
                "file" : {},
                "swift" : {},
            }
        }
    json1.update(json2)
    f.write(json.write(json1, True))
