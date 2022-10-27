#!/bin/env python

import re
import sys
import os
import copy
import socket
from optparse import OptionParser
import tempfile
import subprocess
import json

class InplaceSqlQuery(object):
    '''
inplace_sql.py
    {-f sql_query_file      | --file=sql_query_file}
options:
    -f sql_query_file,     --file=sql_query_file              : required, sql query file

examples:
    ./inplace_sql.py -f sql_queryx
    '''

    def __init__(self):
        self.parser = OptionParser(usage=self.__doc__)

    def usage(self):
        print self.__doc__ % {'prog' : sys.argv[0]}

    def addOptions(self):
        self.parser.add_option('-f', '--file', action='store', dest='sqlQuery')


    def parseParams(self, optionList):
        self.optionList = optionList
        self.addOptions()
        (options, args) = self.parser.parse_args(optionList)
        self.options = options
        ret = self.checkOptionsValidity(options)
        if not ret:
            print "ERROR: checkOptionsValidity Failed!"
            return False
        self.initMember(options)
        return True

    def checkOptionsValidity(self, options):
        if options.sqlQuery == None or options.sqlQuery == '':
            print "ERROR: sql file must be specified"
            return False
        return True


    def initMember(self, options):
        self.sqlQuery = options.sqlQuery

    def replaceQuery(self, sqlQuery, emplaceStr, sqlKv):
        prefix = ""
        if sqlQuery.startswith("/sql?") :
            sqlQuery =  sqlQuery[5:]
            prefix = "/sql?"
        datas = json.loads(emplaceStr);
        data = datas[0]
        for replaceStr in data:
            if type(replaceStr) == unicode:
                sqlQuery = sqlQuery.replace("?", "'" + replaceStr + "'", 1)
            elif type(replaceStr) == int:
                sqlQuery = sqlQuery.replace("?", str(replaceStr), 1)
            elif type(replaceStr) == float:
                sqlQuery = sqlQuery.replace("?", str(replaceStr), 1)
        return prefix + sqlQuery + "&&kvpair=" + str(sqlKv)

    def run(self):
        kvpairSplit = '&&kvpair='
        fo = open(self.sqlQuery, "r")
        for line in fo.readlines():
            if line.find("dynamic_params:[") == -1:
                continue
            spVec = line.split(kvpairSplit)
            if len(spVec) != 2 :
                continue
            kvVec = spVec[1].split(";")
            newKvStr = ""
            for kv in kvVec:
                kvSplit =  kv.split(":")
                if len(kvSplit) != 2 :
                    continue
                if kvSplit[0] != "dynamic_params":
                    if len(newKvStr) == 0:
                        newKvStr = kv
                    else:
                        newKvStr += ";" + kv
                    continue
                print self.replaceQuery(spVec[0], kvSplit[1], newKvStr)
        return "", "", 0

if __name__ == '__main__':
    cmd = InplaceSqlQuery()
    if len(sys.argv) < 2:
        cmd.usage()
        sys.exit(-1)
    if not cmd.parseParams(sys.argv):
        cmd.usage()
        sys.exit(-1)
    data, error, code = cmd.run()
    if code != 0:
        if error:
            print error
        sys.exit(code)
    sys.exit(0)
