#!/bin/env python
import os
import sys

HERE = os.path.split(os.path.realpath(__file__))[0]

def executCmd(cmd):
    print cmd
    os.system(cmd)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'python build_vector_data.py installRoot'
        print 'please specify install root'
        sys.exit(-1)
    installRoot = sys.argv[1]
    configRoot = os.path.join(HERE, '../config/vector_config/offline_config/')
    dataPath = os.path.join(HERE, '../data/vector.data')
    workDir = os.path.join(HERE, './workdir')
    if not os.path.exists(workDir):
        cmd = 'mkdir ' + workDir
        executCmd(cmd)
    cmd = '%s/usr/local/bin/bs startjob -c %s -n in0 -j local -m full -d %s -w %s -i ./runtimedata -p 1 --documentformat=ha3' % (installRoot, configRoot, dataPath, workDir)
    executCmd(cmd)
