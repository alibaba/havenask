#!/bin/env python
import re, sys, os, socket, tempfile, subprocess, json, time, pprint, logging, httplib, traceback

class FlowGraph(object):
    '''%(prog)s [options]
options:
    -a ADDRESS, --address=ADDRESS : required, specific http address directly
    -A NAME, --appName=NAME       : required, appName in buildId
    -D TABLE, --dataTable=TABLE   : required, dataTable in buildId
    -G ID, --generationId=ID      : required, generationId in buildId
    -T, --printTaskInfo           : optional, print task info in graph

examples:
    %(prog)s -a XXX.XXX.XXX.XXX:6666 -A mainse_bspre_offline -D mainse -G 1571110174
    '''
    def __init__(self):
        self.ossPath = 'invalid'
        self.ossKey = 'invalid'
        self.ossId = 'invalid'
        curTime = int(time.time())
        self.today = self.__toTimeStr(curTime)
        self.yesterday = self.__toTimeStr(curTime - 86400)
        # delete yesterday png files
        cmd = 'osscmd deleteallobject %(ossPath)s%(date)s --id=%(id)s --key=%(key)s --force=true' % {
            'date' : self.yesterday,
            'ossPath' : self.ossPath,
            'id' : self.ossId,
            'key' : self.ossKey
        }
        p = subprocess.Popen(cmd, shell=True, close_fds=False,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()

    def usage(self):
        return self.__doc__ % {
            'prog' : sys.argv[0]
        }

    def __addOptions(self, parser):
        parser.add_option('-a', '--address', action='store', dest='address', default=None)
        parser.add_option('-A', '--appName', action='store', dest='appName', default=None)
        parser.add_option('-D', '--dataTable', action='store', dest='dataTable', default=None)
        parser.add_option('-G', '--generationId', action='store', type='int', dest='generationId', default=None)
        parser.add_option('-T', '--printTaskInfo', action='store_true', dest='printTaskInfo', default=False)

    def parseParams(self, optionList):
        from optparse import OptionParser
        parser = OptionParser(usage=self.usage())
        self.__addOptions(parser)
        (self.options, self.args) = parser.parse_args(optionList)
        logging.debug("%s %s" % (self.options, self.args))
        if self.options.address == None:
            return False
        if self.options.appName == None:
            return False
        if self.options.dataTable == None:
            return False
        if self.options.generationId == None:
            return False
        return True

    def __http_post(self, method, request):
        #fake result
        # f = open("result.txt")
        # ret = json.loads(f.read(), strict=False)
        # f.close()
        # return ret, self.options.address
    
        try:
            conn = httplib.HTTPConnection(self.options.address)
            reqStr = json.dumps(request)
            #print(reqStr)
            conn.request("POST", method, reqStr)
            rep = conn.getresponse()
            if rep.status != 200:
                return None
            ret = json.loads(rep.read(), strict=False)
            conn.close()
            return ret, self.options.address
        except Exception, e:
            print self.options.address, method, request
            return None

    def __getRequest(self):
        if self.options.appName or self.options.dataTable or self.options.generationId:
            request = {'buildId' : {}}
            if self.options.appName:
                request['buildId']['appName'] = self.options.appName
            if self.options.dataTable:
                request['buildId']['dataTable'] = self.options.dataTable
            if self.options.generationId:
                request['buildId']['generationId'] = self.options.generationId
        else:
            request = {}
        if self.options.printTaskInfo:
            request['printTaskInfo'] = True
        return request

    def __toTimeStr(self, timestamp):
        try:
            return time.strftime("%y%m%d", time.localtime(timestamp))
        except Exception, e:
            return str(timestamp)
        
    def run(self):
        method = "/AdminService/printGraph"
        response, address = self.__http_post(method, self.__getRequest())
        dotStr = response.get('response', '')

        tmp = tempfile.NamedTemporaryFile()
        tmp.write(dotStr)
        tmp.flush()

        pngFile = '%s-%s_%d.png' % (self.today, self.options.generationId, int(time.time()))
        cmd = 'dot -Tpng %s -o %s' % (tmp.name, pngFile)
        p = subprocess.Popen(cmd, shell=True, close_fds=False,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        tmp.close()

        cmd = 'osscmd put %(png)s %(ossPath)s%(png)s --id=%(id)s --key=%(key)s' % {
            'png' : pngFile,
            'date' : self.today,
            'ossPath' : self.ossPath,
            'id' : self.ossId,
            'key' : self.ossKey
        }
        # print cmd
        p = subprocess.Popen(cmd, shell=True, close_fds=False,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        httpObject = stdout.split('\n')[1].split(' ')[3]
        os.remove(pngFile)
        return httpObject
            
if __name__ == '__main__':
    flowGraph = FlowGraph()
    if not flowGraph.parseParams(sys.argv):
        print(flowGraph.usage())
        sys.exit(-1)
    result = flowGraph.run()
    if not result:
        sys.exit(-1)
    print result
    sys.exit(0)
 
