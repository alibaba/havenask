#!/bin/env python
# *-* coding:utf-8 *-*
import os
import sys
import urllib

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print 'python curl_http.py qrsHttpPort query'
        sys.exit(-1)
    httpPort = sys.argv[1]
    query = sys.argv[2]

    cmd = "curl 'http://127.0.0.1:%s/sql?%s' " % (httpPort, urllib.quote(query))
    print cmd
    os.system(cmd)
