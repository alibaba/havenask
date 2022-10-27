#!/bin/env python

import re
import sys
import os
from optparse import OptionParser
import tempfile
if sys.getdefaultencoding() != 'utf-8':
    reload(sys)
    sys.setdefaultencoding('utf-8')
import urllib
import zlib

protoLibPath = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
sys.path.insert(1, protoLibPath)
protoPath = os.path.join(os.path.dirname(os.path.realpath(__file__)), '../../../python2.7/site-packages')
sys.path.insert(1, protoPath)

import process
from ha3.proto.BasicDefs_pb2 import *
from ha3.proto.QrsService_pb2 import *
from ha3.proto.PBResult_pb2 import *
import arpc.python.rpc_impl_async as async_rpc_impl

class SearchCmd:
    '''
     search.py
        {-q query_string         | --query=query_string}
        [-s                      | --sql]
        [-a qrs_address          | --qrsaddress=qrs_address]
        [-p protocol_type        | --protocol=protocol_type]
        [-u eagleeye_userdata    | --userdata=eagleeye_userdata]
        [-t eagleeye_traceid     | --traceid=eagleeye_traceid]

options:
    -q query_string, --query=query_string          : required, query string
    -s, --sql                                      : use sql
    -a qrs_address, --qrsaddress=qrs_address       : required, qrs http/tcp address,
    -p protocol_type, --protocol=protocol_type     : optional, http(default) or arpc.
    -u eagleeye_userdata, --userdata=eagleeye_userdata : optional, defalut("")
    -t eagleeye_traceod,  --traceid=eagleeye_traceid : optional, defalut("")

examples:
    ./search.py -s -q "SELECT nid FROM phone"
    ./search.py -q "config=cluster:daogou&&query=phrase:mp3" -a "http://10.250.12.31:6788"
    ./search.py -q "config=cluster:daogou&&query=phrase:mp3" -a "tcp:10.250.12.31:6780" -p arpc
    ./search.py -q "config=cluster:daogou&&query=phrase:mp3" -a "tcp:10.250.12.31:6780" -p arpc
    '''

    def __init__(self):
        self.parser = OptionParser(usage=self.__doc__)

    def addOptions(self):
        self.parser.add_option('-q', '--query', action='store', dest='query')
        self.parser.add_option('-s', '--sql', action='store_true', dest='useSql')
        self.parser.add_option('-a', '--qrsaddress', action='store', dest='qrsAddress')
        self.parser.add_option('-p', '--protocol', type = 'choice', choices= ['http', 'arpc', 'http_header'],
                               action='store', dest='protocolType', default='http')
        self.parser.add_option('', '--readable', action='store_true', dest='readable', default=False)
        self.parser.add_option('-u', '--eagleeye_userdata', action='store',
                               dest='eagleeyeUserdata', default='')
        self.parser.add_option('-t', '--eagleeye_traceid', action='store',
                               dest='eagleeyeTraceId', default='')
        self.parser.add_option('-m', '--bizName', action='store',
                               dest='bizName', default='')
        self.parser.add_option('', '--qrsQueueName', action='store', dest='qrsQueueName')
        self.parser.add_option('', '--timeout', action='store', dest='timeout', type='int')

    def parseParams(self, optionList):
        self.optionList = optionList

        self.addOptions()
        (options, args) = self.parser.parse_args(optionList)
        self.options = options

        ret = self.checkOptionsValidity(options, args)
        if not ret:
            print "ERROR: checkOptionsValidity Failed!"
            return False
        self.initMember(options)
        return True


    def checkOptionsValidity(self, options, args):
        if options.query == None or options.query == '':
            if len(args) > 1:
                options.query = args[1]
        if options.query == None or options.query == '':
            print "ERROR: query must be specified"
            return False
        return True

    def initMember(self, options):
        self.qrsAddr = options.qrsAddress
        self.query = options.query
        self.useSql = options.useSql
        self.protocolType = options.protocolType
        self.protoReadable = options.readable
        self.eagleeyeUserdata = options.eagleeyeUserdata
        self.eagleeyeTraceId = options.eagleeyeTraceId
        self.qrsQueueName = options.qrsQueueName
        self.bizName = options.bizName
        self.timeout = options.timeout

    def run(self):
        if self.qrsAddr == None:
            print "ERROR: can't get qrs address"
            return ("", "ERROR: can't get qrs address", -1)

        if self.protocolType == 'http':
            return self._runHttpSearch()
        elif self.protocolType == 'http_header':
            return self._runHttpHeaderSearch()
        else:
            return self._runArpcSearch()

    def _runHttpSearch(self):
        tmp = tempfile.NamedTemporaryFile()
        headerFile = tempfile.NamedTemporaryFile()
        allQueroy = ''
        if self.useSql:
            allQuery = self.query
        elif self.qrsQueueName or self.timeout or self.eagleeyeTraceId or self.eagleeyeUserdata or self.bizName:
            allQuery = '{\"assemblyQuery\":\"%s\"' % (self.query)
            if self.qrsQueueName:
                allQuery = allQuery + ', \"taskQueueName\":\"' + self.qrsQueueName + '\"'
            if self.timeout:
                allQuery = allQuery + ', \"timeout\":' + str(self.timeout) + ''
            if self.eagleeyeUserdata:
                allQuery = allQuery + ', \"userData\":\"' + self.eagleeyeUserdata + '\"'
            if self.eagleeyeTraceId:
                allQuery = allQuery + ', \"traceId\":\"' + self.eagleeyeTraceId + '\"'
            if self.bizName:
                allQuery = allQuery + ', \"bizName\":\"' + self.bizName + '\"'
            allQuery = allQuery + '}'
        else:
            allQuery = self.query
        tmp.write(allQuery)
        tmp.flush()
        qrsAddr = self.qrsAddr
        if self.useSql:
            if not qrsAddr.endswith('/'):
                qrsAddr += '/'
            qrsAddr += 'sql'
        cmd = 'curl -D %s -X POST %s -d @%s' % (headerFile.name, qrsAddr, tmp.name)
        pro = process.Process()
        (stdout, stderr, code) = pro.run(cmd)
        if code != 0:
            print stderr
        else:
            content = headerFile.readlines()
            for line in content:
                if line.find('Content-Encoding: protobuf') != -1:
                    stdout = self._parseProtobufResult(stdout)
                    break;
            sys.stdout.write(stdout)
            sys.stdout.flush()
        return (stdout, stderr, code)

    def _runHttpHeaderSearch(self):
        tmp = tempfile.NamedTemporaryFile()
        headerFile = tempfile.NamedTemporaryFile()
        allQuery = self.query
        tmp.write(allQuery)
        tmp.flush()
        qrsAddr = self.qrsAddr
        if self.useSql:
            if not qrsAddr.endswith('/'):
                qrsAddr += '/'
            qrsAddr += 'sql'
        if self.eagleeyeTraceId or self.eagleeyeUserdata:
            header = ''
            if self.eagleeyeTraceId:
                header = '-H EagleEye-TraceId:' + self.eagleeyeTraceId
            if self.eagleeyeUserdata:
                header = header + ' -H EagleEye-UserData:' + self.eagleeyeUserdata
            cmd = 'curl -D %s -X POST %s -d @%s %s ' % (headerFile.name, qrsAddr, tmp.name, header)
        else :
            cmd = 'curl -D %s -X POST %s -d @%s' % (headerFile.name, qrsAddr, tmp.name)
        print cmd
        pro = process.Process()
        (stdout, stderr, code) = pro.run(cmd)
        if code != 0:
            print stderr
        else:
            content = headerFile.readlines()
            for line in content:
                if line.find('Content-Encoding: protobuf') != -1:
                    stdout = self._parseProtobufResult(stdout)
                    break;
            sys.stdout.write(stdout)
            sys.stdout.flush()
        return (stdout, stderr, code)

    def _runArpcSearch(self):
        stubWrapper = async_rpc_impl.ServiceStubWrapperFactory.createServiceStub(
            QrsService_Stub, self.qrsAddr)
        request = QrsRequest()
        request.assemblyQuery = self.query
        if self.eagleeyeUserdata != "":
            request.userData = self.eagleeyeUserdata
        if self.eagleeyeTraceId != "":
            request.traceId = self.eagleeyeTraceId
        if self.qrsQueueName :
            request.taskQueueName = self.qrsQueueName
        if self.timeout :
            request.timeout = self.timeout
        if self.bizName :
            request.bizName = self.bizName
        reply = stubWrapper.asyncCall('search', request)

        isTimeOut = False
        try:
            reply.wait(10)
        except async_rpc_impl.ReplyTimeOutError:
            isTimeOut = True

        if isTimeOut:
            print "search with arpc protocol call timeout"
            return "", "search with arpc protocol call timeout", -1
        else:
            response = reply.getResponse()
            if response:
                retStr = self.getResultFromResponse(response)
                sys.stdout.write(retStr)
                sys.stdout.flush()

                return (retStr, "", 0)
            else:
                print "QRS arpc search service does not response, Response is None"
                return "", "QRS arpc search service does not response, Response is None", -1

    def getResultFromResponse(self, response):
        ret = response.assemblyResult
        if response.compressType != CT_NO_COMPRESS:
            ret = zlib.decompress(ret)
        if response.formatType == FT_PROTOBUF:
            ret = self._parseProtobufResult(ret)
        return ret

    def _parseProtobufResult(self, result):
        if not self.protoReadable:
            return result
        pbResult = PBResult()
        try:
            pbResult.ParseFromString(result)
            return str(pbResult)
        except:
            print 'parse protobuf result failed!'
        return result

    def getProtocolType(self):
        # only for test
        return self.protocolType

    def usage(self):
        print self.__doc__ % {'prog' : sys.argv[0]}


if __name__ == '__main__':
    cmd = SearchCmd()

    if not cmd.parseParams(sys.argv):
        cmd.usage()
        sys.exit(-1)

    data, error, code = cmd.run()
    if code != 0:
        if error:
            print error
    sys.exit(0)
