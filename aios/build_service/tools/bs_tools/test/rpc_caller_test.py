#!/bin/env python

import sys
import os
import unittest
curPath = os.path.split(os.path.realpath(__file__))[0]
parentPath = curPath + "/../"
sys.path.append(parentPath)

import rpc_caller
import fake_process

class RpcCallerTest(unittest.TestCase):
    def setUp(self):
        self.rpcCallerExe = 'rpc_caller_exe'
        self.nuwaAddress = 'nuwa_address'
        self.rpcCaller = rpc_caller.RpcCaller(self.rpcCallerExe, self.nuwaAddress)

    def tearDown(self):
        pass

    def testStartJob(self):
        startJobCmd = 'rpc_caller_exe --Server=nuwa_address --Method=StartWorkItem --ParameterFile="/job/config/json"'
        ret = ('', '', 0)
        resultDict = {startJobCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.startJob('/job/config/json')
        self.assertTrue(code == 0)

    def testStartJobInvalidNuwaAddr(self):
        startJobCmd = 'rpc_caller_exe --Server=nuwa_address --Method=StartWorkItem --ParameterFile="/job/config/json"'
        ret = ('Failed to resolve nuwa address', '', 0)
        resultDict = {startJobCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.startJob('/job/config/json')
        self.assertTrue(code == -2)

    def testStopJob(self):
        stopJobCmd = 'rpc_caller_exe --Server=nuwa_address --Method=StopWorkItem --Parameter="nuwa://job_master_address/"'
        ret = ('', '', 0)
        resultDict = {stopJobCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.stopJob('nuwa://job_master_address/')
        self.assertTrue(code == 0)

    def testStopJobInvalidNuwaAddr(self):
        stopJobCmd = 'rpc_caller_exe --Server=nuwa_address --Method=StopWorkItem --Parameter="nuwa://job_master_address/"'
        ret = ('Failed to resolve nuwa address', '', 0)
        resultDict = {stopJobCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.stopJob('nuwa://job_master_address/')
        self.assertTrue(code == -2)

    def testGetJobStatus(self):
        getJobStatusCmd = 'rpc_caller_exe --Server=nuwa://job_master_address/ --Method=GetJobStatus --Parameter="nuwa://job_master_address/"'
        ret = ('', '', 0)
        resultDict = {getJobStatusCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.getJobStatus('nuwa://job_master_address/')
        self.assertTrue(code == 0)

    def testGetJobStatusInvalidNuwaAddr(self):
        getJobStatusCmd = 'rpc_caller_exe --Server=nuwa://job_master_address/ --Method=GetJobStatus --Parameter="nuwa://job_master_address/"'
        ret = ('Failed to resolve nuwa address', '', 0)
        resultDict = {getJobStatusCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.getJobStatus('nuwa://job_master_address/')
        self.assertTrue(code == -2)

    def testGetWorkItemStatus(self):
        getWorkItemStatusCmd = 'rpc_caller_exe --Server=nuwa_address --Method=GetWorkItemStatus --Parameter="nuwa://job_master_address/"'
        ret = ('', '', 0)
        resultDict = {getWorkItemStatusCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.getWorkItemStatus('nuwa://job_master_address/')
        self.assertTrue(code == 0)

    def testGetWorkItemStatusInvalidNuwaAddr(self):
        getWorkItemStatusCmd = 'rpc_caller_exe --Server=nuwa_address --Method=GetWorkItemStatus --Parameter="nuwa://job_master_address/"'
        ret = ('Failed to resolve nuwa address', '', 0)
        resultDict = {getWorkItemStatusCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.getWorkItemStatus('nuwa://job_master_address/')
        self.assertTrue(code == -2)

    def testJobStopped(self):
        jobStoppedCmd = 'rpc_caller_exe --Server=nuwa_address --Method=IsWorkItemFullyStopped --Parameter="job_name"'
        ret = ('', '', 0)
        resultDict = {jobStoppedCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.isJobFullyStopped('job_name')
        self.assertTrue(code == 0)

    def testJobStoppedInvalidNuwaAddr(self):
        jobStoppedCmd = 'rpc_caller_exe --Server=nuwa_address --Method=IsWorkItemFullyStopped --Parameter="job_name"'
        ret = ('Failed to resolve nuwa address', '', 0)
        resultDict = {jobStoppedCmd : ret}
        self.rpcCaller.process = fake_process.FakeProcess(resultDict)
        data, error, code = self.rpcCaller.isJobFullyStopped('job_name')
        self.assertTrue(code == -2)

    def testHandleError(self):
        data, error, code = self.rpcCaller._handleError((rpc_caller.RPC_CALL_WORK_HAS_STARTED_ERROR, '', 0))
        self.assertTrue(code == -1)

        data, error, code = self.rpcCaller._handleError((rpc_caller.RPC_CALL_ADDRESS_ERROR, '', 0))
        self.assertTrue(code == -2)

        data, error, code = self.rpcCaller._handleError((rpc_caller.RPC_CALL_DROP_MESSAGE_ERROR, '', 0))
        self.assertTrue(code == 1)

        data, error, code = self.rpcCaller._handleError((rpc_caller.RPC_CALL_WORK_ITEM_ERROR, '', 0))
        self.assertTrue(code == 2)

        data, error, code = self.rpcCaller._handleError((rpc_caller.RPC_CALL_SERVER_NOT_EXIST, '', 0))
        self.assertTrue(code == 3)

if __name__ == "__main__":
    unittest.main()
