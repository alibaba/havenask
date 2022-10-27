#!/bin/env /usr/bin/python

import os
import sys
curPath = os.path.split(os.path.realpath(__file__))[0]
parentPath = curPath + "/../"
sys.path.append(parentPath)
import unittest

from apsara_job_delegate import ApsaraJobDelegate
from tools_config import *
from build_rule_config import *
from build_app_config import *
from fs_util_delegate import *
from tools_test_common import *
from mock import *

class FakeRpcCaller(object):
    def __init__(self, case):
        pass

jobStatusStr = """connecting to nuwa://10.249.68.27:10240/sys/fuxi/master/ForChildMaster
connected
Method=GetWorkItemStatus
Parameter=nuwa://10.249.68.27:10240/admin/job_name/JobMaster
TraceId=0
TraceLogLevel=ALL
%s
"""

class ApsaraJobDelegateTest(unittest.TestCase):
    def setUp(self):
        self.workDir = TEST_DATA_TEMP_PATH + "job_delegate_test/"
        if os.path.exists(self.workDir):
            shutil.rmtree(self.workDir)
        os.mkdir(self.workDir)

        self.configPath = TEST_DATA_PATH + "job_delegate_test/config/"
        self.toolsConfig = ToolsConfig()
        self.fsUtil = FsUtilDelegate(self.toolsConfig)
        self.buildAppConf = BuildAppConfig(self.fsUtil, self.configPath)

        self.buildAppConf.indexRoot = self.workDir + "index/"
        self.buildAppConf.zookeeperRoot = self.workDir + "zookeeper/"

        self.clusterName = 'cluster1'
        self.buildRuleConf = BuildRuleConfig(self.fsUtil, self.clusterName,
                                             self.configPath)
        self.jobDelegate = ApsaraJobDelegate(
            self.configPath, self.buildAppConf, self.buildRuleConf, 
            self.toolsConfig, self.fsUtil, self.clusterName)
        self.jobDelegate.indexRoot = ''

        self.jobDelegate.rpcCaller = FakeRpcCaller(self)
        self.startJobMock = MagicMock()
        self.stopJobMock = MagicMock()
        self.isJobFullyStoppedMock = MagicMock()
        self.getJobStatusMock = MagicMock()
        self.getWorkItemStatusMock = MagicMock()
        setattr(self.jobDelegate.rpcCaller, 'startJob', self.startJobMock)
        setattr(self.jobDelegate.rpcCaller, 'stopJob', self.stopJobMock)
        setattr(self.jobDelegate.rpcCaller, 'isJobFullyStopped', self.isJobFullyStoppedMock)
        setattr(self.jobDelegate.rpcCaller, 'getJobStatus', self.getJobStatusMock)
        setattr(self.jobDelegate.rpcCaller, 'getWorkItemStatus', self.getWorkItemStatusMock)

    def tearDown(self):
        del self.jobDelegate

    def testGetJobStatusString(self):
        self.checkJobStatusString(jobStatusStr % 'Running', 0, JOB_STATUS_RUNNING)
        self.checkJobStatusString(jobStatusStr % 'Failed', 0, JOB_STATUS_FAILED)
        self.checkJobStatusString(jobStatusStr % 'Terminated', 0, JOB_STATUS_SUCCESS)
        self.checkJobStatusString(jobStatusStr % 'Waiting', 0, JOB_STATUS_WAITING)
        self.checkJobStatusString('WorkItem not found', 255, JOB_STATUS_NOT_RUNNING)

    def checkJobStatusString(self, jobStr, code, expectStatus):
        self.getWorkItemStatusMock.return_value = (jobStr, '', code)
        ret, status = self.jobDelegate._ApsaraJobDelegate__getJobStatusString(
            'job_master_address')
        self.assertTrue(ret)
        self.assertEqual(expectStatus, status)

    def testGetJobStatusStringFailed(self):
        self.getWorkItemStatusMock.return_value = ('Failed to resolve nuwa address', '', -1)
        ret, status = self.jobDelegate._ApsaraJobDelegate__getJobStatusString(
            'job_master_address', 5, 1)
        self.assertEqual(5, self.getWorkItemStatusMock.call_count)
        self.assertFalse(ret)
        self.assertEqual(JOB_STATUS_UNKNOWN, status)

    def testStartJob(self):
        self.startJobMock.return_value = ('', '', 0)
        setattr(self.jobDelegate, 'replaceTemplate', self.__mockReplaceTemplate)
        self.jobDelegate.buildMode = BUILD_MODE_FULL
        self.assertTrue(self.jobDelegate.doStartJob(JOB_BUILD_STEP, 2, 2))

    def __mockReplaceTemplate(self, templateName, fileName, paramDict):
        self.assertTrue('JobParameterString' in paramDict)
        jobConfStr = paramDict['JobParameterString']
        self.assertTrue(jobConfStr.find(BUILD_MODE) != -1 \
                            and jobConfStr.find(BUILD_MODE_FULL) != -1)
        return True

    def testStopJob(self):
        self.stopJobMock.return_value = ('WorkItem not found', '', -1)
        self.assertTrue(self.jobDelegate.stopJob())

    def testStopJobFailed(self):
        self.stopJobMock.return_value = ('Unknown return', '', -1)
        self.assertFalse(self.jobDelegate.stopJob())

    def testStopJobCheckJobFullyStopped(self):
        self.stopJobMock.return_value = ('', '', 0)
        self.isJobFullyStoppedMock.return_value = (jobStatusStr % 'true', '', 0)
        self.assertTrue(self.jobDelegate.stopJob())

    def testStopJobCheckJobFullyStoppedFailed(self):
        self.stopJobMock.return_value = ('', '', 0)
        self.isJobFullyStoppedMock.side_effect = [(jobStatusStr % 'false', '', 0),
                                                  (jobStatusStr % 'true', '', 0),
                                                  (jobStatusStr % 'false', '', 0),
                                                  (jobStatusStr % 'true', '', 0),
                                                  (jobStatusStr % 'false', '', 0),
                                                  (jobStatusStr % 'true', '', -1)]
        self.assertFalse(self.jobDelegate.stopJob())

    def testWaitJobFinish(self):
        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'Terminated', None, 0)
        self.jobDelegate.waitJobFinish(5, JOB_BUILD_STEP, 1)
        self.assertEqual(1, self.getWorkItemStatusMock.call_count)

    def testWaitJobFinishFailedRet(self):
        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'Terminated', None, -1)
        self.assertRaises(Exception, self.jobDelegate.waitJobFinish, 5, JOB_BUILD_STEP, 1)

    def testWaitJobFinishFailedStatusFailed(self):
        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'Failed', None, 0)
        self.assertRaises(Exception, self.jobDelegate.waitJobFinish, 5, JOB_BUILD_STEP, 1)

    def testWaitJobFinishFailedStatusNotRunning(self):
        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'WorkItem not found', None, 0)
        self.assertRaises(Exception, self.jobDelegate.waitJobFinish, 5, JOB_BUILD_STEP, 1)

    def testWaitJobFinishFailedTimeOut(self):
        self.assertRaises(Exception, self.jobDelegate.waitJobFinish, 0, JOB_BUILD_STEP, 1)

    def testWaitJobFinishWaiting(self):
        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'Waiting', None, 0)
        self.assertRaises(Exception, self.jobDelegate.waitJobFinish, 5, JOB_BUILD_STEP, 1)
        self.assertEqual(5, self.getWorkItemStatusMock.call_count)

    def testGetJobStatus(self):
        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'Terminated', None, 0)
        self.assertTrue(self.jobDelegate._ApsaraJobDelegate__getJobStatusFromJobMaster(None, False))
        self.getJobStatusMock.return_value = (jobStatusStr % 'Terminated', None, 0)
        self.assertTrue(self.jobDelegate._ApsaraJobDelegate__getJobStatusFromJobMaster(None, True))

    def testGetJobStatusWorkItem(self):
        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'WorkItem not found', None, 1)
        self.assertTrue(self.jobDelegate._ApsaraJobDelegate__getJobStatusFromJobMaster(None, False))
        self.getJobStatusMock.return_value = (jobStatusStr % 'WorkItem not found', None, 0)
        self.assertTrue(self.jobDelegate._ApsaraJobDelegate__getJobStatusFromJobMaster(None, False))

    def testGetJobStatusFailed(self):
        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'Terminated', None, 1)
        self.assertFalse(self.jobDelegate._ApsaraJobDelegate__getJobStatusFromJobMaster(None, False))
        self.getJobStatusMock.return_value = (jobStatusStr % 'Terminated', None, 1)
        self.assertFalse(self.jobDelegate._ApsaraJobDelegate__getJobStatusFromJobMaster(None, False))

    def testIsJobRunning(self):
        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'UnKnown', None, 1)
        self.assertEqual(JOB_STATUS_UNKNOWN, self.jobDelegate.isJobRunning(10))

        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'Waiting', None, 0)
        self.assertEqual(JOB_STATUS_RUNNING, self.jobDelegate.isJobRunning(10))

        self.getWorkItemStatusMock.return_value = (jobStatusStr % 'WorkItem not found', None, 0)
        self.assertEqual(JOB_STATUS_RUNNING, self.jobDelegate.isJobRunning(10))

def suite():
    suite = unittest.makeSuite(ApsaraJobDelegateTest, 'test')
    return suite

if __name__ == "__main__":
    unittest.main()
