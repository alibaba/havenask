#!/bin/env python
import socket
from rpc_controller import ANetRpcController
from rpc_channel_async import ANetRpcChannel

import threading
import sys
import time

class ANetRpcImpl(object):
    @classmethod
    def OpenChannel(self, spec):
        #parse spec
        argList = spec.split(':');
        if len(argList) != 3:
            print "ERROR:server address format is {tcp|udp}:ip:port, example \"tcp:127.0.0.1:9876\""
            return None

        #create socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        except socket.error, msg:
            print "ERROR: create socket exception %s" % msg
            return None

        #connect to server
        try:
            s.connect((argList[1], int(argList[2])))
        except socket.error, msg:
            s.close()
            #print "ERROR: connect to %s exception %s" % (spec, msg)
            return None
        
        #create RpcChannel
        return ANetRpcChannel(s)

    @classmethod
    def Controller(self):
        return ANetRpcController()

class ReplyTimeOutError(Exception):
    def __init__(self, msg = ''):
        self._msg = "ReplyTimeOutError " + msg

    def __str__(self):
        return str(self._msg)

class Reply(object):
    def __init__(self, controller = None):
        self._response = None
        self._controller = controller
        self._isDone = False
        self._cond = threading.Condition()
    
    def __call__(self, response = None):
        self._cond.acquire()
        self._response = response
        self._isDone = True
        self._cond.notify()
        self._cond.release()

    def wait(self, timeOut = None):
        self._cond.acquire()
        isTimeOut = False
        if timeOut == None:
            while not self._isDone:
                self._cond.wait()
        else:
            deadline = time.time() + timeOut
            while not self._isDone:
                delay = deadline - time.time()
                if delay <= 0:
                    isTimeOut = True
                    break
                self._cond.wait(delay)
        self._cond.release()
        if isTimeOut:
            raise ReplyTimeOutError()

    def getErrorText(self):
        if self._controller != None:
            return self._controller.ErrorText()
        return ''

    def getResponse(self):
        return self._response

class ServiceStubWrapper:
    def __init__(self, stub):
        self._stub = stub

    def syncCall(self, method, request, timeOut = None):
        reply = self.asyncCall(method, request)
        reply.wait(timeOut)
        return reply.getResponse()
    
    def asyncCall(self, method, request):
        methodObj = getattr(self._stub, method)
        if methodObj == None:
            print "ERROR: stub [%s] has no method [%s]" % (
                self._stub.GetDescriptor().name, method)
            return None
        
        controller = ANetRpcImpl.Controller()
        reply = Reply(controller)
        methodObj(controller, request, reply)
        return reply

import connection_creator
import rpc_channel_async

class ServiceStubWrapperFactory:
    @classmethod
    def createServiceStub(self, serviceClassName, serverSpec):
        conn = connection_creator.ConnectionCreator.createConnection(serverSpec)
        if not conn.connect():
            print "connect to server [%s] failed" % serverSpec
            return None
        
        rpcChannel = rpc_channel_async.ANetRpcChannel(conn)
        stub = serviceClassName(rpcChannel)
        stubWrapper = ServiceStubWrapper(stub)
        return stubWrapper


if __name__ == "__main__":
    #sys.path.append("/home/zhouhb/deploy_express_test_bed/usr/local/lib/python/site-packages/")
    from deploy_express.proto.Deploy_pb2 import *

    replys = []
    stubWrappers = []
    for i in range(0, 20):
        deployStubWrapper = ServiceStubWrapperFactory.createServiceStub(
            DeployService_Stub, "tcp:10.250.8.133:9090")
        if deployStubWrapper == None:
            sys.exit(1)

        stubWrappers.append(deployStubWrapper)
        request = GetJobStatusRequest()
        replys.append(deployStubWrapper.asyncCall("getStatus", request))

    for reply in replys:
        reply.wait(10)
        
        response = reply.getResponse()
        if response == None:
            print reply.getErrorText()
        else:
            print response
