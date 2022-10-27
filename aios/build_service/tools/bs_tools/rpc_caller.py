#!/bin/env python

import re
from process import *
#rpc_caller method types
RPC_CALL_STARTWORKITEM_METHOD = 'StartWorkItem'
RPC_CALL_STOPWORKITEM_METHOD = 'StopWorkItem'
RPC_CALL_GETWORKITEMSTATUS_METHOD = 'GetWorkItemStatus'
RPC_CALL_ISWORKITEMFULLYSTOPPED_METHOD = 'IsWorkItemFullyStopped'
RPC_CALL_GETJOBSTATUS = 'GetJobStatus'

#rpc_caller error types
RPC_CALL_ADDRESS_ERROR = 'Failed to resolve nuwa address'
RPC_CALL_DROP_MESSAGE_ERROR = 'Message is dropped by KFC'
RPC_CALL_WORK_ITEM_ERROR = 'WorkItem not found'
RPC_CALL_WORK_HAS_STARTED_ERROR = 'WorkItem with the same name already existing'
RPC_CALL_SERVER_NOT_EXIST = 'local server does not exist'

class RpcCaller:
    def __init__(self, rpcCallerExe, address):
        self.address = address
        self.rpcCallerExe = rpcCallerExe
        self.process = Process()

    def startJob(self, jobConfFileName):
        parameter = 'ParameterFile=' + self._quote_argument(jobConfFileName)
        cmd = self._createCmdString(RPC_CALL_STARTWORKITEM_METHOD, parameter, self.address)
        return self._handleCmd(cmd)

    def stopJob(self, jobMasterAddress):
        parameter = 'Parameter=' + self._quote_argument(jobMasterAddress)
        cmd = self._createCmdString(RPC_CALL_STOPWORKITEM_METHOD, parameter, self.address)
        return self._handleCmd(cmd)

    def isJobFullyStopped(self, jobName):
        parameter = 'Parameter=' + self._quote_argument(jobName)
        cmd = self._createCmdString(RPC_CALL_ISWORKITEMFULLYSTOPPED_METHOD, parameter, self.address)
        return self._handleCmd(cmd)        

    def getJobStatus(self, jobMasterAddr):
        parameter = "Parameter=" + self._quote_argument(jobMasterAddr)
        cmd = self._createCmdString(RPC_CALL_GETJOBSTATUS, parameter, jobMasterAddr)
        return self._handleCmd(cmd)

    def getWorkItemStatus(self, jobMasterAddr):
        parameter = "Parameter=" + self._quote_argument(jobMasterAddr)
        cmd = self._createCmdString(RPC_CALL_GETWORKITEMSTATUS_METHOD, parameter, self.address)
        return self._handleCmd(cmd)

    def _createCmdString(self, method, parameter, serverAddress):
        cmd = '%(rpc_caller)s --Server=%(server_address)s --Method=%(method)s --%(parameter)s' %\
            {'rpc_caller': self.rpcCallerExe,\
             'server_address': serverAddress,\
             'method': method,\
             'parameter': parameter }
        return cmd

    def _quote_argument(self, argument):
        return '"%s"' % (
            argument
            .replace('\\', '\\\\')
            .replace('"', '\\\"')
            .replace('$', '\\\$')
            .replace('`', '\\\`')
            )

    def _handleCmd(self, cmd):
        return self._handleError(self.process.run(cmd))

    def _handleError(self, ret):
        data, error, code = ret
        if data.find(RPC_CALL_WORK_HAS_STARTED_ERROR) != -1:
            error = error + ' ' + RPC_CALL_WORK_HAS_STARTED_ERROR
            code = -1
            # why None?
            return None, error, code

        if data.find(RPC_CALL_ADDRESS_ERROR) != -1:
            error = error + ' ' + RPC_CALL_ADDRESS_ERROR
            code = -2
        elif data.find(RPC_CALL_DROP_MESSAGE_ERROR) != -1:
            error = error + ' ' + RPC_CALL_DROP_MESSAGE_ERROR
            code = 1
        elif data.find(RPC_CALL_WORK_ITEM_ERROR) != -1:
            error = error + ' ' + RPC_CALL_WORK_ITEM_ERROR
            code = 2
        elif data.find(RPC_CALL_SERVER_NOT_EXIST) != -1:
            error = error + ' ' + RPC_CALL_SERVER_NOT_EXIST
            code = 3
        return data, error, code
