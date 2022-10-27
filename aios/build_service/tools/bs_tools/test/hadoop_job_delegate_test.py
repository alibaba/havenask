#!/bin/env /usr/bin/python
import sys
import os

curPath = os.path.split(os.path.realpath(__file__))[0]
parentPath = curPath + "/../"
sys.path.append(parentPath)
sys.path.append(parentPath + "../")

import unittest
from hadoop_job_delegate import HadoopJobDelegate
from tools_config import *
from build_rule_config import *
from build_app_config import *
from fs_util_delegate import *
from tools_test_common import *
from mock import *
from hadoop_util import HadoopUtil
import fake_hadoop_util
import fake_file_util
import hadoop_job_delegate

class MockHadoopJobDelegate(hadoop_job_delegate.HadoopJobDelegate):
     def __init__(self, configPath, buildAppConf, 
                  buildRuleConf, toolsConfig, fsUtil, clusterName):
          super(MockHadoopJobDelegate, self).__init__(
               configPath, buildAppConf, 
               buildRuleConf, toolsConfig, fsUtil, clusterName)
          
     def _getFilesForJob(self, builderExe):
          return True, ""
     
     def _HadoopJobDelegate__getJobId(self, outFileName):
          return "job name"

     # for control output for this func
     def _HadoopJobDelegate__getJobInfo(self, step, timeout=30):
          if step == 'need output false':
               return False, ''
          if step == 'need output true':
               return True, ''
          if step == 'need output true and jobId success':
               return True, 'job_success'
          if step == 'need output true and jobId running':
               return True, 'job_running'
          if step == 'need output true and jobId failed':
               return True, 'job_failed'
          if step == 'need output true and jobId accept':
               return True, 'job_accept'
          if step == 'need output true and jobId killed':
               return True, 'job_killed'
          if step == 'need output true and jobId unknown':
               return True, 'job_unknown'

     def _HadoopJobDelegate__isJobRunning(self, jobId, timeout=30):
          if jobId == '' or jobId == 'job_running':
               return True
          else:
               return False
          
     def _HadoopJobDelegate__initPipesOutputFileName(self, jobName):
          if jobName == 'build':
               return 'normal ret'
          else:
               return 'exception ret'
          
     def _HadoopJobDelegate__getJobId(self, outFileName):
          if outFileName == 'normal ret':
               return 'jobId'
          else:
               return None
     
     def _HadoopJobDelegate__writeJobInfo(self, jobId, jobName, jobInfoFileName):
          pass

     def getLibraryPath(self, hadoopJobPackagePath):
          return ''

     def determineJobNameAndBuilderExe(self, step):
          if step == 'build':
               return HADOOP_BUILD_EXE, 'build'
          else:
               return HADOOP_MERGE_EXE, 'merge'
     
          
class HadoopJobDelegateTest(unittest.TestCase):
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
          self.jobDelegate = HadoopJobDelegate(self.configPath, self.buildAppConf, 
                                               self.buildRuleConf, self.toolsConfig, 
                                               self.fsUtil, self.clusterName)
          self.mockJobDelegate = MockHadoopJobDelegate(self.configPath, self.buildAppConf, 
                                                       self.buildRuleConf, self.toolsConfig, 
                                                       self.fsUtil, self.clusterName)
     
     def tearDown(self):
          pass

     def testGetLibraryPath(self):
          hadoopJobPackagePath = 'packagepath'
          self.assertRaises(Exception, self.jobDelegate.getLibraryPath, hadoopJobPackagePath)

          hadoopJobPackagePath = '/package/path/'
          self.assertEqual('./', self.jobDelegate.getLibraryPath(hadoopJobPackagePath))

          hadoopJobPackagePath = '/package/path//'
          self.assertEqual('./', self.jobDelegate.getLibraryPath(hadoopJobPackagePath))

          hadoopJobPackagePath = '/package/path'
          self.assertEqual('./path', self.jobDelegate.getLibraryPath(hadoopJobPackagePath))
          
     def testGetJobBaseDir(self):
          path = self.jobDelegate._HadoopJobDelegate__getJobBaseDir('build')
          self.assertEqual(self.workDir + 'index/cluster1/_bs_hd_build_tmp_dir_/', path)
          
     def testGetJobTemplateName(self):
          self.assertEqual('hadoop_build_job.xml', 
                           self.jobDelegate._HadoopJobDelegate__getJobTemplateName(JOB_BUILD_STEP))
          self.assertEqual('hadoop_merge_job.xml', 
                           self.jobDelegate._HadoopJobDelegate__getJobTemplateName(JOB_MERGE_STEP))
          self.assertEqual('hadoop_merge_job.xml', 
                           self.jobDelegate._HadoopJobDelegate__getJobTemplateName(JOB_END_MERGE_STEP))
     
     def testIsJobRunning(self):
          #fake hadoop_job_util job id is job_201103101836_0026 and job_201103101836_0025
          hadoopUtil = fake_hadoop_util.FakeHadoopUtil("")
          self.jobDelegate._setHadoopUtil(hadoopUtil)
          self.assertTrue(self.jobDelegate._HadoopJobDelegate__isJobRunning('job_201103101836_0025'))
          self.assertFalse(self.jobDelegate._HadoopJobDelegate__isJobRunning('job_201103101836_0027'))
          self.assertRaises(Exception, self.jobDelegate._HadoopJobDelegate__isJobRunning, 'job_201103101836_0025', 0)

     def testGetJobInfo(self):
          self.jobDelegate.fsUtil = fake_file_util.FakeFileUtil()
          self.jobDelegate.fsUtil.setNotExistPath(self.workDir + 'index/cluster1/_bs_hd_build_tmp_dir_/_hadoop_build_job.info')
          jobInfo, content = self.jobDelegate._HadoopJobDelegate__getJobInfo('build')
          self.assertTrue(jobInfo)
          self.assertEqual('', content)
          
          self.jobDelegate.fsUtil.setFakeContent('content')
          jobInfo, content = self.jobDelegate._HadoopJobDelegate__getJobInfo('merge')       
          self.assertTrue(jobInfo)
          self.assertEqual('content', content)

          self.jobDelegate.fsUtil.setFakeContent('content1\ncontent2')
          jobInfo, content = self.jobDelegate._HadoopJobDelegate__getJobInfo('merge')       
          self.assertFalse(jobInfo)
          self.assertEqual('', content)

          self.jobDelegate.fsUtil.setFakeContent('content1')
          jobInfo, content = self.jobDelegate._HadoopJobDelegate__getJobInfo('merge', 0)
          self.assertFalse(jobInfo)
          self.assertEqual('', content)

     def testCheckJobRunning(self):
          hadoopUtil = fake_hadoop_util.FakeHadoopUtil("")
          self.mockJobDelegate._setHadoopUtil(hadoopUtil)

          self.assertEqual(JOB_STATUS_UNKNOWN, self.mockJobDelegate._checkJobRunning('need output false'))
          self.assertEqual(JOB_STATUS_NOT_RUNNING, self.mockJobDelegate._checkJobRunning('need output true'))
          self.assertEqual(JOB_STATUS_RUNNING, self.mockJobDelegate._checkJobRunning('need output true and jobId running'))
          self.assertEqual(JOB_STATUS_NOT_RUNNING, self.mockJobDelegate._checkJobRunning('need output true and jobId success'))

     def testTranslateJobStatus(self):
          self.assertEqual(JOB_STATUS_SUCCESS, self.jobDelegate._translateJobStatus('SUCCEEDED'))
          self.assertEqual(JOB_STATUS_RUNNING, self.jobDelegate._translateJobStatus('RUNNING'))
          self.assertEqual(JOB_STATUS_FAILED, self.jobDelegate._translateJobStatus('FAILED'))
          self.assertEqual(JOB_STATUS_WAITING, self.jobDelegate._translateJobStatus('ACCEPTED'))
          self.assertEqual(JOB_STATUS_FAILED, self.jobDelegate._translateJobStatus('KILLED'))
          self.assertEqual(JOB_STATUS_UNKNOWN, self.jobDelegate._translateJobStatus('UNKNOWN'))

     def testDoGetJobStatus2(self):
          hadoopUtil = fake_hadoop_util.FakeHadoopUtil("")
          self.mockJobDelegate._setHadoopUtil(hadoopUtil)
          status, jobStatus = self.mockJobDelegate._HadoopJobDelegate__doGetJobStatus2(True, 'need output false', 30)
          self.assertEqual(False, status)
          self.assertEqual(JOB_STATUS_UNKNOWN, jobStatus)

          status, jobStatus = self.mockJobDelegate._HadoopJobDelegate__doGetJobStatus2(True, 'need output true', 30)
          self.assertEqual(True, status)
          self.assertEqual(JOB_STATUS_NOT_RUNNING, jobStatus)

          status, jobStatus = self.mockJobDelegate._HadoopJobDelegate__doGetJobStatus2(True, 'need output true and jobId success', 30)
          self.assertEqual(True, status)
          self.assertEqual(JOB_STATUS_SUCCESS, jobStatus)

          status, jobStatus = self.mockJobDelegate._HadoopJobDelegate__doGetJobStatus2(True, 'need output true and jobId running', 30)
          self.assertEqual(True, status)
          self.assertEqual(JOB_STATUS_RUNNING, jobStatus)
          
     def testDoGetJobStatus1(self):
          hadoopUtil = fake_hadoop_util.FakeHadoopUtil("")
          self.mockJobDelegate._setHadoopUtil(hadoopUtil)
          self.mockJobDelegate.fsUtil = fake_file_util.FakeFileUtil()

          status, jobStatus = self.mockJobDelegate._HadoopJobDelegate__doGetJobStatus1(True, 'need output false', 30)
          self.assertEqual(False, status)
          self.assertEqual(JOB_STATUS_UNKNOWN, jobStatus)

          status, jobStatus = self.mockJobDelegate._HadoopJobDelegate__doGetJobStatus1(True, 'need output true', 30)
          self.assertEqual(True, status)
          self.assertEqual(JOB_STATUS_NOT_RUNNING, jobStatus)

          status, jobStatus = self.mockJobDelegate._HadoopJobDelegate__doGetJobStatus1(True, 'need output true and jobId success', 30)
          self.assertEqual(False, status)
          self.assertEqual(JOB_STATUS_UNKNOWN, jobStatus)

          status, jobStatus = self.mockJobDelegate._HadoopJobDelegate__doGetJobStatus1(True, 'need output true and jobId running', 30)
          self.assertEqual(True, status)
          self.assertEqual(JOB_STATUS_RUNNING, jobStatus)

     def testGetJobHistoryInfo(self):
          self.jobDelegate.fsUtil = fake_file_util.FakeFileUtil()
          self.jobDelegate.fsUtil.setNotExistPath('not/exist/path')
          ret, jobStatus, detail = self.jobDelegate._getJobHistoryInfo(True, 'not/exsit/path')
          self.assertFalse(ret)
          self.assertEqual(None, jobStatus)
          self.assertEqual(None, detail)
          
          hadoopUtil = fake_hadoop_util.FakeHadoopUtil("")
          self.jobDelegate._setHadoopUtil(hadoopUtil)
          ret, jobStatus, detail = self.jobDelegate._getJobHistoryInfo(True, 'exsit/path')
          self.assertFalse(ret)
          self.assertEqual(None, jobStatus)
          self.assertEqual(None, detail)

          hadoopUtil.setJobHistory('not exsit status')
          self.jobDelegate._setHadoopUtil(hadoopUtil)
          ret, jobStatus, detail = self.jobDelegate._getJobHistoryInfo(True, 'exsit/path')
          self.assertFalse(ret)
          self.assertEqual(None, jobStatus)
          self.assertEqual(None, detail)
          
          hadoopUtil.setJobHistory('Status: SUCCESS\n')
          self.jobDelegate._setHadoopUtil(hadoopUtil)
          ret, jobStatus, detail = self.jobDelegate._getJobHistoryInfo(True, 'exsit/path')
          self.assertFalse(ret)
          self.assertEqual(None, jobStatus)
          self.assertEqual(None, detail)
          
          jobStatus = 'Status: SUCCESS\n(Hadoop job\n\nTask Summary)'
          hadoopUtil.setJobHistory(jobStatus)
          self.jobDelegate._setHadoopUtil(hadoopUtil)
          ret, jobStatus, detail = self.jobDelegate._getJobHistoryInfo(True, 'exsit/path')
          self.assertTrue(ret)
          self.assertEqual(JOB_STATUS_SUCCESS, jobStatus)
          self.assertEqual('Hadoop job\n', detail)

     def testStopJob(self):
          self.mockJobDelegate.fsUtil = fake_file_util.FakeFileUtil()
          self.mockJobDelegate.fsUtil.setNotExistPath('not/exist/path')

          self.assertFalse(self.mockJobDelegate._HadoopJobDelegate__doStopJob('need output false'))
          self.assertTrue(self.mockJobDelegate._HadoopJobDelegate__doStopJob('need output true'))
          self.assertTrue(self.mockJobDelegate._HadoopJobDelegate__doStopJob('need output true and jobId success'))
          
     def testStartJob(self):
          hadoopUtil = fake_hadoop_util.FakeHadoopUtil("")
          self.mockJobDelegate._setHadoopUtil(hadoopUtil)
          self.mockJobDelegate.fsUtil = fake_file_util.FakeFileUtil()
          self.assertRaises(Exception, self.mockJobDelegate.doStartJob, JOB_MERGE_STEP, 1, 1)
          
          # test normal situation
          self.mockJobDelegate.indexRoot = 'index/root'
          self.mockJobDelegate.buildAppConf.hadoopPackageName = 'pkg'
          self.mockJobDelegate.doStartJob(JOB_BUILD_STEP, 1, 1)
