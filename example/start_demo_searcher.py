#!/bin/env python
# *-* coding:utf-8 *-*

import os
import sys
HERE = os.path.split(os.path.realpath(__file__))[0]


def executCmd(cmd):
    print cmd
    os.system(cmd)

if __name__ == '__main__':
    os.system("pkill -9 sap_server_d")
    if len(sys.argv) < 2:
        print 'python start_demo_searcher.py installRoot case[optional] qrsHttpPort[optional]'
        print 'please specify install root'
        sys.exit(-1)
    installRoot = os.path.abspath(sys.argv[1])
    if len(sys.argv) > 2:
        case = sys.argv[2]
    else:
        case = "normal"
    caseRoot = os.path.abspath(os.path.join(HERE, "cases", case))
    workDir = os.path.join(caseRoot, './workdir')
    workDir = os.path.abspath(workDir)
    
    qrsHttpPort = '45800'
    if len(sys.argv) > 3:
        qrsHttpPort = sys.argv[3]

    try:
        sys.path.append(os.path.abspath(
            os.path.join(HERE, "..", caseRoot)))
        from setup import search_prepare
        configDir, testQueries = search_prepare() 
    except Exception as e:
        print(e)
        print("failed to call build prepare function in %s/setup.py" % (caseRoot))
        exit(-1)                      
    configDir = os.path.abspath(configDir)
    bizsConfigDir = os.path.join(configDir, 'bizs')

    if not os.path.exists(workDir):
        print '%s not exists, please build index with build_demo_data.py first.'
        sys.exit(-1)
    cmd = 'cd %s && python %s/usr/local/lib/python/site-packages/ha_tools/local_search_starter.py -i ./runtimedata/ -c %s -p 30468,30480 -b %s --qrsHttpArpcBindPort %s' % (workDir, installRoot, configDir, installRoot, qrsHttpPort)

    executCmd(cmd)
    
    curl_tool = os.path.abspath(os.path.join(HERE, "common", "curl_http.py"))
    for query in testQueries:
        cmd = "python %s %s \"%s\"" %(curl_tool, qrsHttpPort, query)
        executCmd(cmd)
