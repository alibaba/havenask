#!/bin/env python

import os
curPath = os.path.split(os.path.realpath(__file__))[0]
parentPath = curPath + "/../"
import sys
sys.path.append(parentPath)
import shutil
import unittest
from tools_test_common import *
from tools_config import *
from build_rule_config import *
from build_app_config import *
from job_delegate import *
from fs_util_delegate import *
import json
import fake_file_util

class FakeJobDelegate(JobDelegate):
    def __init__(self, configPath, buildAppConf, buildRuleConf,
                 toolsConfig, fsUtil, clusterName):
        super(FakeJobDelegate, self).__init__(configPath, buildAppConf, buildRuleConf,
                                              toolsConfig, fsUtil, clusterName)
        self.getJobStatusRet = []
        self.startBuildJobRet = []
        self.startMergeJobRet = []
        self.startEndMergeJobRet = []
        self.isJobRunningRet = []
        self.waitJobFinishRet = []

        self.getJobStatusParam = []
        self.startBuildJobParam = []
        self.startMergeJobParam = []
        self.startEndMergeJobParam = []
        self.waitJobFinishParam = []

    def doGetJobStatus(self, showDetail, step, timeout):
        self.getJobStatusParam.append((showDetail, step, timeout))
        ret = self.getJobStatusRet[0]
        self.getJobStatusRet = self.getJobStatusRet[1:]
        return ret

    def doStartJob(self, step, mapInstanceCount, reduceInstanceCount):
        if step == JOB_BUILD_STEP:
            return self.startBuildJob(mapInstanceCount, reduceInstanceCount)
        elif step == JOB_MERGE_STEP:
            return self.startMergeJob(mapInstanceCount, reduceInstanceCount)
        else:
            return self.startEndMergeJob(mapInstanceCount, reduceInstanceCount)

    def startBuildJob(self, mapInstanceCount, reduceInstanceCount):
        self.startBuildJobParam.append((mapInstanceCount, reduceInstanceCount))
        ret = self.startBuildJobRet[0]
        if ret and hasattr(self, 'indexRoot') and self.indexRoot != self.buildAppConf.indexRoot:
            self.fsUtil.mkdir('%s/%s/generation_%d/' % (self.indexRoot,
                                                        self.clusterName,
                                                        self.generationId) , True)
        self.startBuildJobRet = self.startBuildJobRet[1:]
        return ret

    def startMergeJob(self, mapInstanceCount, reduceInstanceCount):
        self.startMergeJobParam.append((mapInstanceCount, reduceInstanceCount))
        ret = self.startMergeJobRet[0]
        self.startMergeJobRet = self.startMergeJobRet[1:]
        return ret

    def startEndMergeJob(self, mapInstanceCount, reduceInstanceCount):
        self.startEndMergeJobParam.append((mapInstanceCount, reduceInstanceCount))
        ret = self.startEndMergeJobRet[0]
        self.startEndMergeJobRet = self.startEndMergeJobRet[1:]
        return ret

    def isJobRunning(self):
        ret = self.isJobRunningRet[0]
        self.isJobRunningRet = self.isJobRunningRet[1:]
        return ret

    def waitJobFinish(self, timeout, step, sleepInterval):
        self.waitJobFinishParam.append((timeout, step, sleepInterval))
        ret = self.waitJobFinishRet[0]
        self.waitJobFinishRet = self.waitJobFinishRet[1:]
        return ret

class JobDelegateTest(unittest.TestCase):
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
        self.jobDelegate = FakeJobDelegate(
            self.configPath, self.buildAppConf, self.buildRuleConf,
            self.toolsConfig, self.fsUtil, self.clusterName)


    def tearDown(self):
        if os.path.exists(self.workDir):
            shutil.rmtree(self.workDir)

    def testStartFullJob(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=True)
        self.fsUtil.mkdir(self.buildAppConf.indexRoot + "cluster1/generation_1/")
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertEqual(0, self.jobDelegate.buildPartFrom)
        self.assertEqual(5, self.jobDelegate.buildPartCount)
        self.assertTrue(self.fsUtil.exists(self.buildAppConf.zookeeperRoot))
        self.assertTrue(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_2/"))
        self.assertEqual(2, self.jobDelegate.generationId)
        self.assertEqual([(5, 0)], self.jobDelegate.startBuildJobParam)
        self.assertEqual([(5, 5)], self.jobDelegate.startMergeJobParam)
        self.__checkAllFunctionCalled()

        jobParamDict = json.loads(self.jobDelegate.generateJobParamStr())
        self.assertEqual(jobParamDict[BUILD_MODE], self.jobDelegate.buildMode)
        self.assertEqual(jobParamDict[CONFIG_PATH], self.jobDelegate.configPath)
        self.assertEqual(jobParamDict[GENERATION], str(self.jobDelegate.generationId))
        self.assertEqual(jobParamDict[CLUSTER_NAME], str(self.jobDelegate.clusterName))
        self.assertTrue(jobParamDict[SRC_SIGNATURE] != '')
        self.assertEqual(jobParamDict[BUILD_RULE_PARTITION_COUNT],
                         str(self.jobDelegate.buildRuleConf.partitionCount))
        self.assertEqual(jobParamDict[BUILD_RULE_BUILD_PARALLEL_NUM],
                         str(self.jobDelegate.buildRuleConf.buildParallelNum))
        self.assertEqual(jobParamDict[BUILD_RULE_MERGE_PARALLEL_NUM],
                         str(self.jobDelegate.buildRuleConf.mergeParallelNum))
        self.assertEqual(jobParamDict[BUILD_RULE_MAP_REDUCE_RATIO],
                         str(self.jobDelegate.buildRuleConf.mapReduceRatio))
        self.assertEqual(jobParamDict[BUILD_RULE_NEED_PARTITION],
                         str(self.jobDelegate.buildRuleConf.needPartition))
        self.assertEqual(jobParamDict[INDEX_ROOT_PATH], self.jobDelegate.indexRoot)
        self.assertEqual(jobParamDict[BUILD_PART_FROM], str(self.jobDelegate.buildPartFrom))
        self.assertEqual(jobParamDict[BUILD_PART_COUNT], str(self.jobDelegate.buildPartCount))
        self.assertEqual(jobParamDict[MERGE_CONFIG_NAME], 'full')
        self.assertEqual(jobParamDict[BUILD_APP_ZOOKEEPER_ROOT], self.buildAppConf.zookeeperRoot)

    def testStartFullJobWithZipConfig(self):
        self.configPath = TEST_DATA_PATH + "job_delegate_test/config_with_zip/"
        self.buildRuleConf = BuildRuleConfig(self.fsUtil, self.clusterName,
                                             self.configPath)
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=True)
        self.fsUtil.mkdir(self.buildAppConf.indexRoot + "cluster1/generation_1/")
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertEqual(0, self.jobDelegate.buildPartFrom)
        self.assertEqual(5, self.jobDelegate.buildPartCount)
        self.assertTrue(self.fsUtil.exists(self.buildAppConf.zookeeperRoot))
        self.assertTrue(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_2/"))
        self.assertEqual(2, self.jobDelegate.generationId)
        self.assertEqual([(5, 0)], self.jobDelegate.startBuildJobParam)
        self.assertEqual([(5, 5)], self.jobDelegate.startMergeJobParam)
        self.__checkAllFunctionCalled()

        jobParamDict = json.loads(self.jobDelegate.generateJobParamStr())
        self.assertEqual(jobParamDict[BUILD_MODE], self.jobDelegate.buildMode)
        self.assertEqual(jobParamDict[CONFIG_PATH], self.jobDelegate.configPath)
        self.assertEqual(jobParamDict[GENERATION], str(self.jobDelegate.generationId))
        self.assertEqual(jobParamDict[CLUSTER_NAME], str(self.jobDelegate.clusterName))
        self.assertTrue(jobParamDict[SRC_SIGNATURE] != '')
        self.assertEqual(jobParamDict[BUILD_RULE_PARTITION_COUNT],
                         str(self.jobDelegate.buildRuleConf.partitionCount))
        self.assertEqual(jobParamDict[BUILD_RULE_BUILD_PARALLEL_NUM],
                         str(self.jobDelegate.buildRuleConf.buildParallelNum))
        self.assertEqual(jobParamDict[BUILD_RULE_MERGE_PARALLEL_NUM],
                         str(self.jobDelegate.buildRuleConf.mergeParallelNum))
        self.assertEqual(jobParamDict[BUILD_RULE_MAP_REDUCE_RATIO],
                         str(self.jobDelegate.buildRuleConf.mapReduceRatio))
        self.assertEqual(jobParamDict[BUILD_RULE_NEED_PARTITION],
                         str(self.jobDelegate.buildRuleConf.needPartition))
        self.assertEqual(jobParamDict[INDEX_ROOT_PATH], self.jobDelegate.indexRoot)
        self.assertEqual(jobParamDict[BUILD_PART_FROM], str(self.jobDelegate.buildPartFrom))
        self.assertEqual(jobParamDict[BUILD_PART_COUNT], str(self.jobDelegate.buildPartCount))
        self.assertEqual(jobParamDict[MERGE_CONFIG_NAME], 'full')
        self.assertEqual(jobParamDict[BUILD_APP_ZOOKEEPER_ROOT], self.buildAppConf.zookeeperRoot)

    def testStartJobParallelBuild(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=True)
        self.buildRuleConf.buildParallelNum = 3
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertEqual([(15, 0)], self.jobDelegate.startBuildJobParam)
        self.assertEqual([(5, 5)], self.jobDelegate.startMergeJobParam)

        self.__checkAllFunctionCalled()

    def testStartJobPBuildPMerge(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=True)
        self.buildRuleConf.buildParallelNum = 3
        self.buildRuleConf.mergeParallelNum = 2
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertEqual([(15, 0)], self.jobDelegate.startBuildJobParam)
        self.assertEqual([(5, 10)], self.jobDelegate.startMergeJobParam)
        self.assertEqual([(5, 1)], self.jobDelegate.startEndMergeJobParam)

        self.__checkAllFunctionCalled()

    def testStartJobBuildPartFrom(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=True)

        self.buildRuleConf.buildParallelNum = 3
        self.buildRuleConf.mergeParallelNum = 2
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, 1, 3, 'full')
        self.assertEqual(1, self.jobDelegate.buildPartFrom)
        self.assertEqual(3, self.jobDelegate.buildPartCount)
        self.assertEqual([(9, 0)], self.jobDelegate.startBuildJobParam)
        self.assertEqual([(3, 6)], self.jobDelegate.startMergeJobParam)
        self.assertEqual([(3, 1)], self.jobDelegate.startEndMergeJobParam)

        self.__checkAllFunctionCalled()

    def testStartJobNeedPart(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=True)
        self.buildRuleConf.needPartition = True
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertEqual(0, self.jobDelegate.buildPartFrom)
        self.assertEqual(5, self.jobDelegate.buildPartCount)
        self.assertEqual([(15, 5)], self.jobDelegate.startBuildJobParam)
        self.assertEqual([(5, 5)], self.jobDelegate.startMergeJobParam)

        self.__checkAllFunctionCalled()

    def testStartJobNeedPartBuildPartFrom(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=True)
        self.buildRuleConf.buildParallelNum = 3
        self.buildRuleConf.mergeParallelNum = 2
        self.buildRuleConf.needPartition = True
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, 1, 3, 'full')
        self.assertEqual(1, self.jobDelegate.buildPartFrom)
        self.assertEqual(3, self.jobDelegate.buildPartCount)
        self.assertEqual([(45, 9)], self.jobDelegate.startBuildJobParam)
        self.assertEqual([(3, 6)], self.jobDelegate.startMergeJobParam)
        self.assertEqual([(3, 1)], self.jobDelegate.startEndMergeJobParam)

        self.__checkAllFunctionCalled()

    def testStartJobIncBuild(self):
        self.__initStartJobRet(runBuildJob=True)
        self.fsUtil.mkdir(self.buildAppConf.indexRoot + "cluster1/generation_0/")
        self.buildRuleConf.buildParallelNum = 3
        self.buildRuleConf.mergeParallelNum = 2
        self.buildRuleConf.needPartition = True
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_INC, JOB_BUILD_STEP, None, None, 'full')
        self.assertFalse(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_1/"))
        self.assertEqual([(15, 5)], self.jobDelegate.startBuildJobParam)
        self.assertEqual(0, self.jobDelegate.generationId)
        self.assertEqual(1, self.buildRuleConf.buildParallelNum)
        self.assertEqual(1, self.buildRuleConf.mergeParallelNum)
        self.__checkAllFunctionCalled()

    def testStartJobWithBuildStep(self):
        self.__initStartJobRet(runBuildJob=True)
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, JOB_BUILD_STEP, None, None, 'full')
        self.assertTrue(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_0/"))
        self.assertEqual([(5, 0)], self.jobDelegate.startBuildJobParam)

        self.__checkAllFunctionCalled()

    def testStartJobWithMergeStep(self):
        self.fsUtil.mkdir(self.buildAppConf.indexRoot + "cluster1/generation_0/")
        self.__initStartJobRet(runMergeJob=True)
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, JOB_MERGE_STEP, None, None, 'full')
        self.assertEqual([(5, 5)], self.jobDelegate.startMergeJobParam)

        self.__checkAllFunctionCalled()

    def testStartJobWithEndMergeStepNoParallelMerge(self):
        self.fsUtil.mkdir(self.buildAppConf.indexRoot + "cluster1/generation_0/")
        self.__initStartJobRet(runEndMergeJob=True)
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, 'end_merge', None, None, 'full')
        self.__checkAllFunctionCalled()

    def testStartJobWithEndMergeStep(self):
        self.fsUtil.mkdir(self.buildAppConf.indexRoot + "cluster1/generation_0/")
        self.__initStartJobRet(runEndMergeJob=True)
        self.buildRuleConf.mergeParallelNum = 2
        self.jobDelegate.startJob(None, 10, 1, BUILD_MODE_FULL, 'end_merge', None, None, 'full')
        self.assertEqual([(5, 1)], self.jobDelegate.startEndMergeJobParam)

        self.__checkAllFunctionCalled()

    def testStartJobWithJobRunning(self):
        self.__initStartJobRet()
        self.jobDelegate.isJobRunningRet = [JOB_STATUS_RUNNING]
        self.assertRaises(Exception,
                          self.jobDelegate.startJob,
                          None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertFalse(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_0/"))
        self.__checkAllFunctionCalled()

    def testStartJobWithGetStatusUnknown(self):
        self.__initStartJobRet()
        self.jobDelegate.isJobRunningRet = [JOB_STATUS_UNKNOWN]
        self.assertRaises(Exception,
                          self.jobDelegate.startJob,
                          None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertFalse(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_0/"))
        self.__checkAllFunctionCalled()

    def testStartJobWithGenerationMeta(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=True)
        generationMeta = TEST_DATA_PATH + 'job_delegate_test/generation_meta'
        self.fsUtil.mkdir(self.buildAppConf.indexRoot+'/cluster1/generation_0/partition_0_20', True)
        self.fsUtil.mkdir(self.buildAppConf.indexRoot+'/cluster1/generation_0/partition_30_50', True)
        self.fsUtil.mkdir(self.buildAppConf.indexRoot+'/cluster1/generation_0/partition_a_b', True)
        self.fsUtil.mkdir(self.buildAppConf.indexRoot+'/cluster1/generation_0/partition_100_200', True)
        self.fsUtil.copy(TEST_DATA_PATH + 'job_delegate_test/empty_generation_meta',
                         self.buildAppConf.indexRoot+'/cluster1/generation_0/partition_100_200')
        setattr(self.jobDelegate, '_JobDelegate__prepareTempIndexDir', self.__doNothing)
        setattr(self.jobDelegate, '_renameTempIndexDir', self.__doNothing)
        setattr(self.jobDelegate, '_JobDelegate__initGenerationId', self.__defaultGenerationId)
        setattr(self.jobDelegate, 'indexRoot', self.buildAppConf.indexRoot)
        self.jobDelegate.startJob(generationMeta, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')

        self.__checkAllFunctionCalled()

    def testStartJobWithJobRunning(self):
        self.__initStartJobRet()
        self.jobDelegate.isJobRunningRet = [JOB_STATUS_RUNNING]
        self.assertRaises(Exception,
                          self.jobDelegate.startJob,
                          None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertFalse(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_0/"))
        self.__checkAllFunctionCalled()

    def testStartJobBuildFailed(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True)
        self.jobDelegate.getJobStatusRet = [(True,JOB_STATUS_FAILED)]
        self.jobDelegate.startMergeJobRet = []
        self.jobDelegate.waitJobFinishRet = [True]
        self.assertRaises(Exception,
                          self.jobDelegate.startJob,
                          None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertFalse(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_0/"))
        self.__checkAllFunctionCalled()

    def testStartJobMergeFailed(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=False)
        self.jobDelegate.getJobStatusRet[1] = (True,JOB_STATUS_FAILED)
        self.assertRaises(Exception,
                          self.jobDelegate.startJob,
                          None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertFalse(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_0/"))
        self.__checkAllFunctionCalled()

    def testStartJobEndMergeFailed(self):
        self.__initStartJobRet(runBuildJob=True, runMergeJob=True, runEndMergeJob=True)
        self.jobDelegate.getJobStatusRet[2] = (True,JOB_STATUS_FAILED)
        self.buildRuleConf.mergeParallelNum = 2
        self.assertRaises(Exception,
                          self.jobDelegate.startJob,
                          None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, None, None, 'full')
        self.assertFalse(self.fsUtil.exists(self.buildAppConf.indexRoot + "cluster1/generation_0/"))
        self.__checkAllFunctionCalled()

    def testStartJobGenerationFailed(self):
        self.jobDelegate.fsUtil = fake_file_util.FakeFileUtil()
        partition = ['partition_0_16383', 'partition_16384_32767',
                     'partition_32768_49151']
        self.jobDelegate.fsUtil.setFakeDirList(partition)
        self.jobDelegate.buildRuleConf.partitionCount = 4
        self.assertRaises(Exception, self.jobDelegate.startJob, None, 10, 1, BUILD_MODE_FULL, JOB_BOTH_STEP, 0, 2, 'full', 1)


    ########################### testGetJobStatus #########################
    def testGetJobStatus(self):
        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_SUCCESS)] * 3
        self.assertTrue(self.jobDelegate.getJobStatus(JOB_BOTH_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_SUCCESS)] * 3
        self.buildRuleConf.mergeParallelNum = 2
        self.assertTrue(self.jobDelegate.getJobStatus(JOB_BOTH_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_SUCCESS)] * 1
        self.assertTrue(self.jobDelegate.getJobStatus(JOB_BOTH_STEP, BUILD_MODE_INC, True))
        self.__checkAllFunctionCalled()

        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_SUCCESS)] * 1
        self.assertTrue(self.jobDelegate.getJobStatus(JOB_BUILD_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_SUCCESS)] * 1
        self.assertTrue(self.jobDelegate.getJobStatus(JOB_MERGE_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_SUCCESS)] * 1
        self.assertTrue(self.jobDelegate.getJobStatus(JOB_END_MERGE_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

    def testGetJobStatusFailed(self):
        self.jobDelegate.getJobStatusRet = [(False, JOB_STATUS_UNKNOWN)] * 1
        self.assertFalse(self.jobDelegate.getJobStatus(JOB_BOTH_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_SUCCESS),
                                            (False, JOB_STATUS_SUCCESS)]
        self.assertFalse(self.jobDelegate.getJobStatus(JOB_BOTH_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_SUCCESS),
                                            (True, JOB_STATUS_SUCCESS),
                                            (False, JOB_STATUS_SUCCESS)]
        self.buildRuleConf.mergeParallelNum = 2
        self.assertFalse(self.jobDelegate.getJobStatus(JOB_BOTH_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_FAILED)]
        self.assertTrue(self.jobDelegate.getJobStatus(JOB_BOTH_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

        self.jobDelegate.getJobStatusRet = [(True, JOB_STATUS_SUCCESS),
                                            (True, JOB_STATUS_FAILED)]
        self.assertTrue(self.jobDelegate.getJobStatus(JOB_BOTH_STEP, BUILD_MODE_FULL, True))
        self.__checkAllFunctionCalled()

    def testCheckPartitionFromAndCount(self):
        # right situation
        self.jobDelegate.fsUtil = fake_file_util.FakeFileUtil()
        partition = ['partition_0_16383', 'partition_16384_32767',
                     'partition_32768_49151']
        self.jobDelegate.fsUtil.setFakeDirList(partition)
        self.jobDelegate._checkPartitionFromAndCount(4, 3, 1, '', JOB_MERGE_STEP)

        # partition has been built, raise exception
        self.assertRaises(Exception,
                          self.jobDelegate._checkPartitionFromAndCount, 4, 2, 2, '', JOB_BUILD_STEP)

    def testCheckGeneration(self):
        # right situation
        self.jobDelegate.fsUtil = fake_file_util.FakeFileUtil()
        self.jobDelegate.fsUtil.setFakeContent('{\"partition_count\" : 2}')
        self.jobDelegate._checkGeneration('', 2, 0, 1, JOB_MERGE_STEP)

        #partition count not equal, raise exception
        self.assertRaises(Exception, self.jobDelegate._checkGeneration, '', 3, 0, 1, JOB_MERGE_STEP)

    def testRenameTempIndexDir(self):
        self.jobDelegate.generationId = 1
        self.jobDelegate.indexRoot = self.workDir + 'temp/'
        self.fsUtil.mkdir(self.jobDelegate.indexRoot + '/cluster1/generation_1', True)
        self.jobDelegate._renameTempIndexDir(BUILD_MODE_FULL, JOB_BOTH_STEP)
        self.assertTrue(self.fsUtil.exists(self.jobDelegate.buildAppConf.indexRoot + 'cluster1/generation_1'))
        self.assertTrue(self.fsUtil.exists(self.jobDelegate.buildAppConf.indexRoot + 'cluster1/generation_1/partition_count'))
        self.assertFalse(self.fsUtil.exists(self.jobDelegate.indexRoot + 'cluster1/generation_1'))

    def testRenameTempIndexDirExist(self):
        self.jobDelegate.generationId = 1
        self.jobDelegate.indexRoot = self.workDir + 'temp/'
        self.jobDelegate.buildAppConf.indexRoot = self.workDir + 'indexRoot/'
        self.fsUtil.mkdir(self.jobDelegate.indexRoot + '/cluster1/generation_1', True)
        self.fsUtil.mkdir(self.jobDelegate.buildAppConf.indexRoot +
                          '/cluster1/generation_1/partition_32768_65535', True)
        self.fsUtil.mkdir(self.jobDelegate.indexRoot +
                          '/cluster1/generation_1/partition_0_32767', True)

        self.jobDelegate._renameTempIndexDir(BUILD_MODE_FULL, JOB_BOTH_STEP)

        self.assertTrue(self.fsUtil.exists(self.jobDelegate.buildAppConf.indexRoot + 'cluster1/generation_1/partition_0_32767'))
        self.assertTrue(self.fsUtil.exists(self.jobDelegate.buildAppConf.indexRoot + 'cluster1/generation_1/partition_32768_65535'))
        self.assertFalse(self.fsUtil.exists(self.jobDelegate.indexRoot + 'cluster1/generation_1/'))

    ########################### test helper function #####################
    def __doNothing(self, buildMode, buildStep):
        pass

    def __defaultGenerationId(self, buildMode, buildStep):
        return 0

    def __checkAllFunctionCalled(self):
        self.assertEqual(0, len(self.jobDelegate.getJobStatusRet))
        self.assertEqual(0, len(self.jobDelegate.startBuildJobRet))
        self.assertEqual(0, len(self.jobDelegate.startMergeJobRet))
        self.assertEqual(0, len(self.jobDelegate.startEndMergeJobRet))
        self.assertEqual(0, len(self.jobDelegate.isJobRunningRet))
        self.assertEqual(0, len(self.jobDelegate.waitJobFinishRet))

    def __initStartJobRet(self, runBuildJob=False, runMergeJob=False, runEndMergeJob=False):
        runJobCount = 0
        if runBuildJob:
            runJobCount += 1
        if runMergeJob:
            runJobCount += 1
        if runEndMergeJob:
            runJobCount += 1
        self.jobDelegate.getJobStatusRet = [(True,JOB_STATUS_SUCCESS)] * runJobCount
        self.jobDelegate.startBuildJobRet = [True] * runBuildJob
        self.jobDelegate.startMergeJobRet = [True] * runMergeJob
        self.jobDelegate.startEndMergeJobRet = [True] * runEndMergeJob
        self.jobDelegate.isJobRunningRet = [JOB_STATUS_NOT_RUNNING]
        self.jobDelegate.waitJobFinishRet = [True] * runJobCount

if __name__ == "__main__":
    unittest.main()
