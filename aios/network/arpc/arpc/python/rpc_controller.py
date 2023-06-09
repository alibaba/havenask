#!/bin/env python

from google.protobuf.service import RpcController

class ANetRpcController(RpcController):
    def __init__(self):
        self.Reset()

    def Reset(self):
        self.failed = False;
        self.reason = "";
        self.canceled = False;
        self.cancelList = []
        
    def Failed(self):
        return self.failed

    def ErrorText(self):
        return self.reason

    def StartCancel(self):
        self.canceled = True;
        for callback in self.cancelList:
            callback.Run()

    def SetFailed(self, reason):
        self.failed = True
        self.reason = reason

    def IsCanceled(self):
        return self.canceled

    def NotifyOnCancel(self, callback):
        self.callbackList.append(callback)

