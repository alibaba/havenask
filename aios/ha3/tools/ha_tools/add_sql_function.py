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

class AddSqlFuctionCmd(object):
    '''
add_sql_function.py
    {-f sql_func_file      | --file=sql_func_file}
    {-n func_name          | --name=func_name}
    {-t udf_type           | --udfType=udf_type}
    {-v version            | --version=version}
    {-d deter              | --deterministic=deter}
    {-a acc_type           | --accType=accType}
    {-p proto_type         | --protoType=protoType}

options:
    -f sql_func_file,     --file=sql_func_file              : required, sql func file
    -n func_name,         --name=fucn_name                  : required, sql func name
    -t udf_type,          --udfType=udf_type                : optional, func type, default UDF 
    -v version,           --version=version                 : optional, function version, default 0.1 
    -d deter,             --deterministic=deterministic     : optional, deterministic, default 1 
    -a acc_type,          --accType=accType                 : optional, used in udf_type = UDAF 
    -p proto_type,        --protoType=protoType             : optional, first is return type, format int32:false,int64:true,string:false;int64:true,string:false

examples:
    ./add_sql_function.py -f sql_func.json -n contain -p boolean:false,int32:false,string:false;boolean:false,int64:false,string:false
0    '''

    def __init__(self):
        self.parser = OptionParser(usage=self.__doc__)

    def usage(self):
        print self.__doc__ % {'prog' : sys.argv[0]}

    def addOptions(self):
        self.parser.add_option('-f', '--file', action='store', dest='sqlFile')
        self.parser.add_option('-n', '--name', action='store', dest='funcName')
        self.parser.add_option('-t', '--udfType', action='store', dest='udfType', default = "UDF")
        self.parser.add_option('-v', '--version', action='store', dest='version', default = "0.1")
        self.parser.add_option('-d', '--deterministic', action='store', dest='deter',
                               type = 'int', default = 1)
        self.parser.add_option('-a', '--accType', action='store', dest='accType')
        self.parser.add_option('-p', '--protoType', action='store', dest='protoType')


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
        if options.sqlFile == None or options.sqlFile == '':
            print "ERROR: sql file must be specified"
            return False

        if options.funcName == None or options.funcName == '':
            print "ERROR: func name must be specified"
            return False
        return True


    def initMember(self, options):
        self.sqlFile = options.sqlFile
        self.funcName = options.funcName
        self.udfType = options.udfType
        self.version = options.version
        self.deter = options.deter
        self.accType = options.accType
        self.protoType = options.protoType

    def loadFuncInfo(self):
        fo = open(self.sqlFile, "r")
        content = fo.read()
        fo.close()
        if content == '':
            content = "{ \"functions\":[]}"
        self.funcInfos = json.loads(content)
        self.funcInfoMap = {}
        for funcInfo in self.funcInfos["functions"]:
            self.funcInfoMap[funcInfo["function_name"]] = funcInfo
            
    def dumpFuncInfo(self):
        funcInfos = {}
        funcInfos["functions"] = self.funcInfoMap.values()
        content = json.dumps(funcInfos, sort_keys=True, indent=4, separators=(',', ': '))
        fo = open(self.sqlFile, "rw+")
        fo.write(content)
        fo.close()

    def parseArgs(self, paramStrs):
        params = []
        if paramStrs is None:
            return params, False
        for paramStr in paramStrs.split(";"):
            oneFunc = []
            for kvs in paramStr.split(","):
                kvSplits = kvs.split(":")
                if len(kvSplits) != 2:
                    return params, False
                vauleMap = {}
                vauleMap["type"] = kvSplits[0]
                if kvSplits[1] == 'true':
                    vauleMap["is_multi"] = True
                else:
                    vauleMap["is_multi"] = False
                oneFunc.append(vauleMap)
            params.append(oneFunc)
        return params, True
    
    def run(self):
        self.loadFuncInfo()
        funcInfo = {}
        if self.udfType == "UDAF":
            params, ret = self.parseArgs(self.accType)
            if not ret or len(params) != 1:
                return "", "", 1
            funcInfo["acc_types"] = params[0]
        params, ret = self.parseArgs(self.protoType)
        if not ret:
            return "", "", 1
        protoVec = []
        for param in params:
            protoMap = {}
            if len(param) <= 1:
                return "", "", 1
            protoMap["returns"] = param[0:1]
            protoMap["params"] = param[1:]
            protoVec.append(protoMap)
        funcInfo["function_name"] = self.funcName
        funcInfo["function_type"] = self.udfType
        funcInfo["is_deterministic"] = self.deter
        funcInfo["function_content_version"] = self.version
        funcInfo["prototypes"] = protoVec
        self.funcInfoMap[self.funcName] = funcInfo
        print "add func %s success, content:%s" % (self.funcName,
                                                   json.dumps(funcInfo, sort_keys=True, indent=4, separators=(',', ': ')))
        self.dumpFuncInfo()
        return "", "", 0

if __name__ == '__main__':
    cmd = AddSqlFuctionCmd()
    if len(sys.argv) < 4:
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
