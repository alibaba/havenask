#!/bin/env python
from google.protobuf.service import RpcChannel
import socket
import struct

class CallContext:
    def __init__(self, responseClass, done):
        self.responseClass = responseClass
        self.done = done

class ANetRpcChannel(RpcChannel):
    def __init__(self, connection):
        self._connection = connection

    def __del__(self):
        self._connection.close()

    def CallMethod(self, methodDescriptor, rpcController, 
                   request, responseClass, done):
        context = CallContext(responseClass, done)
        ret = self._connection.post(methodDescriptor, request, context)
        if not ret:
            rpc_controller.SetFailed("send packet error")
            done()
            return False
        
        return True
