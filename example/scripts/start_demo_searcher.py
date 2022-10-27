#!/bin/env python
# *-* coding:utf-8 *-*

import os
import sys

HERE = os.path.split(os.path.realpath(__file__))[0]

def executCmd(cmd):
    print cmd
    os.system(cmd)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print 'python start_demo_searcher.py installRoot qrsHttpPort[optional]'
        print 'please specify install root'
        sys.exit(-1)
    installRoot = sys.argv[1]
    qrsHttpPort = '45800'
    if len(sys.argv) > 2:
        qrsHttpPort = sys.argv[2]

    workDir = os.path.join(HERE, './workdir')
    configDir = os.path.join(HERE, '../config/normal_config/online_config/')
    if not os.path.exists(workDir):
        print '%s not exists, please build index with build_demo_data.py first.'
        sys.exit(-1)
    cmd = 'cd %s && python %s/usr/local/lib/python/site-packages/ha_tools/local_search_starter.py -i ./runtimedata/ -c %s -p 30468,30480 -b %s --qrsHttpArpcBindPort %s' % (workDir, installRoot, configDir, installRoot, qrsHttpPort)

    executCmd(cmd)
    cmd = '''python curl_http.py %s "query=select id,hits from in0 where MATCHINDEX('title', '搜索词典')" ''' % qrsHttpPort
    executCmd(cmd)
