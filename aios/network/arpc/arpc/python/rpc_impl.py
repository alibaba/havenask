#!/bin/env python
import socket
from rpc_controller import ANetRpcController
from rpc_channel import ANetRpcChannel

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
