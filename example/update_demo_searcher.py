#!/bin/env python
# *-* coding:utf-8 *-*

import os
import sys
HERE = os.path.split(os.path.realpath(__file__))[0]

def executCmd(cmd):
    print(cmd)
    os.system(cmd)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('python update_demo_searcher.py installRoot case configPath')
        print('please specify 3 arguments')
        sys.exit(-1)
    installRoot = os.path.abspath(sys.argv[1])
    case = sys.argv[2]
    configPath = os.path.abspath(sys.argv[3])
    caseRoot = os.path.abspath(os.path.join(HERE, "cases", case))
    workDir = os.path.join(caseRoot, 'workdir')
    indexDir = os.path.join(workDir, "runtimedata")
    os.chdir(workDir)
    
    cmd = '/usr/bin/python %s/usr/local/lib/python/site-packages/havenask_tools/local_search_update.py -i %s -c %s' %(installRoot, indexDir, configPath)

    executCmd(cmd)
    