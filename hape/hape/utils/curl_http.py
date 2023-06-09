#!/bin/env python
# *-* coding:utf-8 *-*
import os
import sys
import urllib

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print 'python curl_http.py address query'
        sys.exit(-1)
    address = sys.argv[1]
    query = sys.argv[2]

    cmd = "curl 'http://%s/sql?%s' " % (address, urllib.quote(query))
    print cmd
    os.system(cmd)
