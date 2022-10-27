# -*- coding: utf-8 -*-

import re
import tempfile
import time
import job_delegate
import rpc_caller
from common_define import *
class ApsaraJobDelegate(job_delegate.JobDelegate):
    def __init__(self, configPath, buildAppConf, buildRuleConf,
                 toolsConfig, fsUtil, clusterName):
        super(ApsaraJobDelegate, self).__init__(configPath, buildAppConf,
                                                buildRuleConf, toolsConfig,
                                                fsUtil, clusterName)
        self._initJobInfo()
        self.rpcCaller = rpc_caller.RpcCaller(toolsConfig.rpc_caller_exe,
                                              self.fuxiMasterAddr)

    def isJobRunning(self, timeout = 60):
        ## return "Running", "NotRunning", "Unknown"
        ret, status = self.__getJobStatusString(self.buildJobMasterAddr, timeout)
        if not ret:
            print "ERROR: failed to check whether build job is running or not."
            return JOB_STATUS_UNKNOWN
        if status != JOB_STATUS_NOT_RUNNING:
            return JOB_STATUS_RUNNING

        ret, status = self.__getJobStatusString(self.mergeJobMasterAddr, timeout)
        if not ret:
            print "ERROR: failed to check whether merge job is running or not."
            return JOB_STATUS_UNKNOWN
        if status != JOB_STATUS_NOT_RUNNING:
            return JOB_STATUS_RUNNING

        ret, status = self.__getJobStatusString(self.endMergeJobMasterAddr, timeout)
        if not ret:
            print "ERROR: failed to check whether end_merge job is running or not."
            return JOB_STATUS_UNKNOWN
        if status != JOB_STATUS_NOT_RUNNING:
            return JOB_STATUS_RUNNING

        return JOB_STATUS_NOT_RUNNING

    def __getTemplateName(self, step, reduceCount):
        if step == JOB_BUILD_STEP:
            if reduceCount != 0:
                return "apsara_build_with_reduce.json"
            else:
                return "apsara_build.json"
        elif step == JOB_MERGE_STEP:
            return "apsara_merge.json"
        elif step == JOB_END_MERGE_STEP:
            return "apsara_end_merge.json"

    def doStartJob(self, step, mapCount, reduceCount):
        templateName = self.__getTemplateName(step, reduceCount)
        tempDir = self.fsUtil.normalizeDir(tempfile.mkdtemp())
        jobConfFileName = tempDir + "job_conf.json"
        paramDict = {}
        paramDict['ReduceInstanceCount'] = reduceCount
        paramDict['MapInstanceCount'] = mapCount
        paramDict['UserName'] = self.buildAppConf.userName
        paramDict['ApsaraBuildJobName'] = self.buildJobName
        paramDict['ApsaraMergeJobName'] = self.mergeJobName
        paramDict['ApsaraEndMergeJobName'] = self.endMergeJobName
        paramDict['ApsaraJobPackage'] = self.buildAppConf.apsaraPackageName
        paramDict["JobParameterString"] = self.generateJobParamStr().replace('"', '\\"')
        paramDict["LD_LIBRARY_PATH"] = ".:/usr/local/lib64:/usr/local/lib:/usr/ali/java/jre/lib/amd64/server/"
        paramDict["HADOOP_HOME"] = self.toolsConfig.hadoop_home

        if not self.replaceTemplate(templateName, jobConfFileName, paramDict):
            return False

        data, error, code = self.rpcCaller.startJob(jobConfFileName)
        if code != 0:
            print "ERROR: start job [%s]" % error
            return False

        return True

    def doGetJobStatus(self, showDetail, step, timeout):
        ret = True
        jobMasterAddr = ""
        if step == JOB_BUILD_STEP:
            jobMasterAddr = self.buildJobMasterAddr
        elif step == JOB_MERGE_STEP:
            jobMasterAddr = self.mergeJobMasterAddr
        elif step == JOB_END_MERGE_STEP:
            jobMasterAddr = self.endMergeJobMasterAddr
        else:
            assert(False)
        tempRet, buildStatus = self.__getJobStatusString(jobMasterAddr, timeout)
        print "INFO: %s job status: %s" % (step, buildStatus)
        ret = ret and tempRet
        if showDetail:
            ret = (self.__getJobStatusFromJobMaster(jobMasterAddr, showDetail) and ret)
        return ret, buildStatus

    def __getJobStatusString(self, jobMasterAddr, timeout = 60, sleepTime = 5):
        code = ""
        error = ""
        while timeout > 0:
            data, error, code = self.rpcCaller.getWorkItemStatus(jobMasterAddr)

            print "INFO: JobStatus: data=[%s] error=[%s]" % (data, error)
            if code != 0:
                if re.search("WorkItem not found", data) is not None:
                    return True, JOB_STATUS_NOT_RUNNING
                else:
                    # the returned message may be "Failed to resolve nuwa address"
                    # it is possible when nuwa server is busy
                    # in this case, just sleep for a while and retry...
                    if timeout < sleepTime:
                        break
                    time.sleep(sleepTime)
                    timeout = timeout - sleepTime
            else:
                return True, self._parseJobStatusString(data)

        print "ERROR: GetWorkItemStatus timeout, retcode[%s], error[%s]" %\
            (code, error)
        return False, JOB_STATUS_UNKNOWN

    def __getJobStatusFromJobMaster(self, jobMasterAddr, showDetail):
        if showDetail:
            data, error, code = self.rpcCaller.getJobStatus(jobMasterAddr)
        else:
            data, error, code = self.rpcCaller.getWorkItemStatus(jobMasterAddr)

        if code != 0:
            if (re.search("WorkItem not found", data) is not None) or \
                    (re.search("Failed to resolve nuwa address", data) is not None):
                print "INFO: job not started or has been stopped."
                return True
            else:
                print error
                return False
        else:
            print data
            return True

    def stopJob(self):
        print "INFO: stop job ... "
        print "INFO: stop build job ... "
        ret = self.__doStopJob(self.buildJobMasterAddr,
                              self.buildJobName)
        print "INFO: stop merge job ... "
        ret = (self.__doStopJob(self.mergeJobMasterAddr,
                               self.mergeJobName) and ret)
        print "INFO: stop end_merge job ... "
        ret = (self.__doStopJob(self.endMergeJobMasterAddr,
                               self.endMergeJobName) and ret)
        return ret

    def __doStopJob(self, jobMasterAddr, jobName):
        data, error, code = self.rpcCaller.stopJob(jobMasterAddr)
        if code != 0:
            print error
            if (re.search("WorkItem not found", data) is not None) or \
                    (re.search("Failed to resolve nuwa address", data) is not None):
                return True
            else:
                return False

        jobName = self.buildAppConf.userName + "/" + jobName
        print "INFO: waiting job to stop... "
        while True:
            time.sleep(0.2)
            data, error, code = self.rpcCaller.isJobFullyStopped(jobName)
            if code != 0:
                print "ERROR: failed to check IsWorkItemFullyStopped."
                return False
            if self._parseStopJobStatusString(data) == "true":
                print "INFO: job has been stopped."
                return True
            time.sleep(1)

    def waitJobFinish(self, timeout, step, sleepInterval):
        timeLeft = timeout
        beforeRun = time.time()
        startTime = self.time2str(beforeRun)
        print "INFO: waiting job to finish...."
        isRunning = False
        isWaiting = False

        masterAddr = ''
        if step == JOB_BUILD_STEP:
            masterAddr = self.buildJobMasterAddr
        elif step == JOB_MERGE_STEP:
            masterAddr = self.mergeJobMasterAddr
        elif step == JOB_END_MERGE_STEP:
            masterAddr = self.endMergeJobMasterAddr
        else:
            assert(False)

        while (timeLeft > 0):
            ret, jobStatus = self.__getJobStatusString(masterAddr, 10, 5)
            if not ret:
                raise Exception("ERROR: faild to get Job Status.")
            else:
                if jobStatus == JOB_STATUS_FAILED :
                    raise Exception("ERROR: job failed")
                elif jobStatus == JOB_STATUS_SUCCESS :
                    afterTime = time.time()
                    endTime = self.time2str(afterTime)
                    print "INFO: job is Finished, start time [%s], end time [%s] time used [%s]" % (startTime, endTime, (afterTime - beforeRun))
                    return
                elif jobStatus == JOB_STATUS_NOT_RUNNING :
                    raise Exception("INFO: job may have been stopped by others.")
                elif jobStatus == JOB_STATUS_WAITING:
                    if not isWaiting:
                        print "INFO: Waiting..."
                        isWaiting = True
                else:
                    if not isRunning:
                        print "INFO: job is Running"
                        isRunning = True
            time.sleep(sleepInterval)
            timeLeft = timeLeft - sleepInterval

        afterTime = time.time()
        endTime = self.time2str(afterTime)
        raise Exception("ERROR: waitJobFinish failed, timeout. detail [timeout:%s, start time:%s, end time:%s, wait time:%s]" % (timeout, startTime, endTime, (afterTime - beforeRun)))

    def _initJobInfo(self):
        #fuxiMasterAddress
        self.fuxiMasterAddr = '%(Nuwa)s/sys/fuxi/master/ForChildMaster' % {
            'Nuwa'    : "nuwa://" + self.buildAppConf.nuwaAddress
            }

        #jobMasterAddress

        self.buildJobMasterAddr = '%(Nuwa)s/%(User)s/%(Job)s/JobMaster' % {
            'Nuwa'    : "nuwa://" + self.buildAppConf.nuwaAddress,
            'User'    : self.buildAppConf.userName,
            'Job' : self.buildJobName
            }

        self.mergeJobMasterAddr = '%(Nuwa)s/%(User)s/%(Job)s/JobMaster' % {
            'Nuwa'    : "nuwa://" + self.buildAppConf.nuwaAddress,
            'User'    : self.buildAppConf.userName,
            'Job' : self.mergeJobName
            }

        self.endMergeJobMasterAddr = '%(Nuwa)s/%(User)s/%(Job)s/JobMaster' % {
            'Nuwa'    : "nuwa://" + self.buildAppConf.nuwaAddress,
            'User'    : self.buildAppConf.userName,
            'Job' : self.endMergeJobName
            }
        return True

    def _parseStopJobStatusString(self, data):
        lines = data.split("\n")
        return lines[6]

    def _parseJobStatusString(self, data):
        lines = data.split("\n")
        status = lines[6]
        if (status == "Failed"):
            return JOB_STATUS_FAILED
        if (status == "Terminated"):
            return JOB_STATUS_SUCCESS
        if (status == "Waiting"):
            return JOB_STATUS_WAITING
        if (status == "Running"):
            return JOB_STATUS_RUNNING
        return JOB_STATUS_UNKNOWN

    #for test
    def setRpcCaller(self, rpcCaller):
        self.rpcCaller = rpcCaller


