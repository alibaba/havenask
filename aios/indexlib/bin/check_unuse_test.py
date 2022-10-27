#!/bin/env python

import sys
import os

allRegistedTestsCmd = "find ../ -name '*test.*' | grep -v debug64 | grep -v release64 | xargs grep -E 'INDEXLIB_UNIT_TEST_CASE|CPPUNIT_TEST' | grep -v newtest.py | grep -v '#define' | awk -F ':' '{print $1 \"(\" $2 }' | awk -F '(' '{print $1 \", \" $3}' | awk -F ', ' '{print $1 \");\" $3}' | awk -F ');' '{print $1, $2}' > tmp_registeredUts"

allTestsCmds = "find ../ -name '*test.*' | grep -v debug64 | grep -v release64 | grep -v newtest.py | xargs grep -E 'void Test*' | awk '{print $1 \"(\" $3}' | awk -F '(' '{print $1, $2}' | awk -F ': ' '{print $1, $2}' > tmp_acturalUts"

if __name__ == "__main__":
    os.system(allRegistedTestsCmd)
    os.system(allTestsCmds)

    registeredUts = set()
    for line in open("tmp_registeredUts"):
        items = line.strip().split(' ')
        fileName = os.path.split(items[0])[1].split('.')[0]
        if len(items) == 2:
            registeredUts.add((fileName, items[1]))
        else:
            print "WARN: unknow line %s"%line.strip()

    for line in open("tmp_acturalUts"):
        items = line.strip().split(' ')
        fileName = os.path.split(items[0])[1].split('.')[0]

        if len(items) == 2:
            if not (fileName, items[1]) in registeredUts:
                print "WARN: [%s, %s]"%(items[0], items[1])
        else:
            print "WARN: unknow line %s"%line.strip()
    
    os.remove("tmp_registeredUts")
    os.remove("tmp_acturalUts")
