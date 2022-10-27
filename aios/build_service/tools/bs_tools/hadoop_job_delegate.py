#!/bin/env python

import re
import os
import glob
import time
import string
import shutil
import tempfile
import job_delegate
from common_define import *
from process import *
from hadoop_util import HadoopUtil

class HadoopJobDelegate(job_delegate.JobDelegate):
    # mr 1.0 start tag: "mapred.JobClient: Running job: "
    # mr 2.0 start tag: "mapreduce.Job: Running job: "
    JOB_ID_START_TAG =  "mapred.*Job.*Running job: "
    JOB_INFO_FILE_NAME = "_hadoop_build_job.info"
    OUTPUT_DIR = "output/"
    def __init__(self, configPath, buildAppConf, buildRuleConf,
                 toolsConfig, fsUtil, clusterName):
        super(HadoopJobDelegate, self).__init__(configPath, buildAppConf,
                                                buildRuleConf, toolsConfig,
                                                fsUtil, clusterName)
        self.hadoopUtil = HadoopUtil(self.toolsConfig.hadoop_home)
        self.installPrefix = self.toolsConfig.install_prefix
        self.TMP_DIR = fsUtil.normalizeDir(tempfile.mkdtemp())
        self.pipesProcess = None
        self.__determineMapReduceVersion(self.toolsConfig.hadoop_home)
        
    def __determineMapReduceVersion(self, hadoop_home):
        hadoop_home = os.path.realpath(hadoop_home)
        check_files = '/'.join([hadoop_home, "share/hadoop/common/*.jar"])
        if glob.glob(check_files):
            self.mapReduceVersion = 2
        else:
            self.mapReduceVersion = 1
        
    def __getJobTemplateName(self, step):
        if (step == JOB_BUILD_STEP):
            return HADOOP_BUILD_JOB_TEMPLATE
        elif step == JOB_MERGE_STEP or step == JOB_END_MERGE_STEP:
            return  HADOOP_MERGE_JOB_TEMPLATE
        assert(False)

    def getLibraryPath(self, hadoopJobPackagePath):
        idx = hadoopJobPackagePath.rfind('/')
        if idx == -1:
            raise Exception("invalid hadoop_job_package: %s" % hadoopJobPackagePath)
        tarball = hadoopJobPackagePath[idx+1:]
        libraryPath = "./%s" % tarball
        return libraryPath

    def determineJobNameAndBuilderExe(self, step):
        if step == JOB_BUILD_STEP:
            return HADOOP_BUILD_EXE, self.buildJobName
        elif step == JOB_MERGE_STEP:
            return HADOOP_MERGE_EXE, self.mergeJobName
        elif step == JOB_END_MERGE_STEP:
            return HADOOP_END_MERGE_EXE, self.endMergeJobName
        
    def doStartJob(self, step, mapCount, reduceCount):
        builderExe, jobName = self.determineJobNameAndBuilderExe(step)
        jobBaseDir = self.__getJobBaseDir(step)
        jobOutputDir = jobBaseDir + self.OUTPUT_DIR
        jobInfoFileName = jobBaseDir + self.JOB_INFO_FILE_NAME

        jobConfFileName = self.TMP_DIR + "job_conf.xml"
        paramDict = {}
        self.indexRoot = self.fsUtil.adaptHadoopPath(self.indexRoot, 
                                                     self.toolsConfig.hadoop_user, 
                                                     self.toolsConfig.hadoop_group,
                                                     self.toolsConfig.hadoop_replica)
        paramDict['JobParameterString'] = self.generateJobParamStr()
        paramDict['hadoop_pipes_executable'] = jobBaseDir + builderExe
        paramDict['hadoop_job_ugi'] = (self.toolsConfig.hadoop_user + 
                                       ", " + self.toolsConfig.hadoop_group)
        paramDict['mapred_job_name'] = jobName
        paramDict['mapred_reduce_tasks'] = reduceCount

        classpathForJob = self.toolsConfig.getClassPath().replace(self.toolsConfig.hadoop_home, "$HADOOP_HOME/")
        childEnv = "CLASSPATH=%s" % classpathForJob
        hadoopJobPackage = self.buildAppConf.hadoopPackageName
        if not hadoopJobPackage:
            raise Exception('hadoop job package cannot be empty')
        libraryPath = self.getLibraryPath(hadoopJobPackage)
        childEnv += ",LD_LIBRARY_PATH=%s:$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64:$JAVA_HOME/lib/amd64:$LD_LIBRARY_PATH" % libraryPath
        paramDict['mapEnv'] = childEnv
        paramDict['mapChildEnv'] = childEnv
        paramDict['reduceEnv'] = childEnv
        paramDict['reduceChildEnv'] = childEnv

        templateName = self.__getJobTemplateName(step)
        if not self.replaceTemplate(templateName, jobConfFileName, paramDict):
            raise Exception("ERROR: replace template")

        localBuildExe = os.path.join(self.installPrefix, "bin", builderExe)
        print "INFO: upload data to hadoop ..."
        self.__prepareHadoopEnv(localBuildExe, jobBaseDir)

        print "INFO: starting job on hadoop..."
        jobInputFile = self.__prepareNlineInputFile(mapCount, self.TMP_DIR, jobBaseDir)

        pipesOutputFileName = self.__initPipesOutputFileName(jobName)
        self.pipesProcess = self.hadoopUtil.runPipes(jobConfFileName, 
                                                     jobInputFile,
                                                     jobOutputDir,
                                                     pipesOutputFileName,
                                                     HADOOP_NLINE_INPUT_FORMAT,
                                                     hadoopJobPackage,
                                                     self.parameters)
        print "INFO: waiting job to start..."
        jobId = self.__getJobId(pipesOutputFileName)
        if jobId is None:
            raise Exception("ERROR: failed to start hadoop job")

        self.__writeJobInfo(jobId, jobName, jobInfoFileName)
        print "INFO: start hadoop job success, %s jobid: %s" % (step, jobId)

    def __initPipesOutputFileName(self, jobName):
        pipesOutputFileName = self.TMP_DIR + jobName + ".output"
        pf = open(pipesOutputFileName, "w")
        pf.close()
        return pipesOutputFileName

    def waitJobFinish(self, timeout, step, sleepInterval):
        timeLeft = timeout
        beforeRun = time.time()
        startTime = self.time2str(beforeRun)
        print "INFO: waiting job to finish...."
        ret, jobId = self.__getJobInfo(step)
        if not ret:
            print "ERROR: get job info failed"
            return False

        if jobId == "":
            print "INFO: no hadoop %s job running." % step
            return True

        while timeLeft > 0:
            if not self.__isJobRunning(jobId):
                afterTime = time.time()
                endTime = self.time2str(afterTime)
                print "INFO: %s job is Finished, start time [%s], end time [%s] time used [%s]" % (step, startTime, endTime, (afterTime - beforeRun))
                return True

            time.sleep(sleepInterval)
            timeLeft = timeLeft - sleepInterval

        afterTime = time.time()
        endTime = self.time2str(afterTime)
        print "ERROR: waitJobFinish failed, timeout. detail [timeout:%s, start time:%s, end time:%s, wait time:%s]" % (timeout, startTime, endTime, (afterTime - beforeRun))
        return False

    def __getJobBaseDir(self, step):
        temp_path = "_bs_hd_%s_tmp_dir_/" % step
        return os.path.join(self.buildAppConf.indexRoot, 
                            self.clusterName,
                            temp_path)

    def __prepareHadoopEnv(self, builderExe, jobBaseDir):
        if self.fsUtil.exists(jobBaseDir):
            self.fsUtil.remove(jobBaseDir)
       
        self.fsUtil.mkdir(jobBaseDir)

        binaryReplica = self.toolsConfig.hadoop_replica
        remoteExeDir = self.fsUtil.adaptHadoopPath(jobBaseDir, 
                                                   self.toolsConfig.hadoop_user, 
                                                   self.toolsConfig.hadoop_group,
                                                   binaryReplica)

        self.fsUtil.copy(builderExe, remoteExeDir)

    def _getFilesForJob(self, builderExe):
        files = ""
        alogConfFile = self.toolsConfig.getHadoopBuilderLogConfFile()
        jobPackage = HadoopJobPackage(builderExe, alogConfFile, 
                                      self.toolsConfig.getLibPath())
        ret, libFilePaths, libFileNames = jobPackage.getDependentLibraries()
        if not ret or len(libFileNames) != len(libFilePaths):
            print "ERROR: failed to getDependentLibraries for %s" % builderExe
            return False, ""
        files += ",".join(libFilePaths)
        return True, files
    
    def stopJob(self):
        print "INFO: stop job ... "
        print "INFO: stop build job ... "
        ret = self.__doStopJob(JOB_BUILD_STEP)
        print "INFO: stop merge job ... "
        ret = self.__doStopJob(JOB_MERGE_STEP) and ret
        print "INFO: stop end merge job ... "
        ret = self.__doStopJob(JOB_END_MERGE_STEP) and ret
        return ret

    def __doStopJob(self, step):
        ret, jobId = self.__getJobInfo(step, 5)
        if not ret:
            print "ERROR: get job info failed"
            return False

        jobBaseDir = self.__getJobBaseDir(step)

        if jobId == "" or not self.__isJobRunning(jobId, 5):
            print "INFO: no hadoop %s job running." % step
            if self.fsUtil.exists(jobBaseDir):
                self.fsUtil.remove(jobBaseDir)
            return True
        
        if not self.hadoopUtil.killJob(jobId):
            print "ERROR: failed to stop hadoop %s job [%s]" % (step, jobId)
            return False

        if self.fsUtil.exists(jobBaseDir):
            self.fsUtil.remove(jobBaseDir)        

        print "INFO: hadoop %s job [%s] stopped." % (step, jobId)
        return True

    # used by mapreduce 1.0
    def _getJobHistoryInfo(self, showDetail, outputDir):
        if not self.fsUtil.exists(outputDir):
            print 'ERROR: job not started or has been killed, output dir not exists!'
            return False, None, None

        info = self.hadoopUtil.getJobHistory(outputDir)
        if info == None:
            print 'ERROR: get job history failed!'
            return False, None, None

        jobStatus = JOB_STATUS_UNKNOWN
        m = re.search('Status: (.*)\n', info)
        if m is None:
            print "ERROR: failed to get job status from history info."
            return False, None, None
        else:
            hadoopStat = m.group(1)
            if hadoopStat == "SUCCESS":
                jobStatus = JOB_STATUS_SUCCESS
            elif hadoopStat == "RUNNING":
                jobStatus = JOB_STATUS_RUNNING
            elif hadoopStat == "FAILED":
                jobStatus = JOB_STATUS_FAILED
                
        pat = ''
        if showDetail:
            pat = '(Hadoop job[\s\S]*)\n\nTask Summary'
        else:
            pat = '(Hadoop job: [\w]*)[\s\S]*(Submitted At:[\s\S]*Status: [a-zA-Z]*)\nCounters: '
            
        detailInfo = ""
        m = re.search(pat, info)
        if m is None:
            print 'ERROR: get job detail failed!'
            return False, None, None
        else:
            for g in m.groups():
                detailInfo += (g + "\n")

        return True, jobStatus, detailInfo

    def doGetJobStatus(self, showDetail, step, timeout):
        if self.mapReduceVersion == 1:
            return self.__doGetJobStatus1(showDetail, step, timeout)
        else:
            return self.__doGetJobStatus2(showDetail, step, timeout)

    # used by mapreduce 1.0    
    def __doGetJobStatus1(self, showDetail, step, timeout):
        ret, jobId = self.__getJobInfo(step, timeout)
        if not ret:
            print "ERROR: get job info failed"
            return False, JOB_STATUS_UNKNOWN
        
        jobStatus = JOB_STATUS_UNKNOWN
        detailInfo = ""
        if jobId == "":
            jobStatus = JOB_STATUS_NOT_RUNNING
        else:
            print "INFO: %s jobid: %s" % (step, jobId)
            if not self.__isJobRunning(jobId, 5):
                jobBaseDir = self.__getJobBaseDir(step)
                jobOutputDir = jobBaseDir + self.OUTPUT_DIR
                ret, jobStatus, detailInfo  = self._getJobHistoryInfo(
                    showDetail, jobOutputDir)
                if not ret:
                    return False, JOB_STATUS_UNKNOWN
            else:
                detailInfo, err, code = self.hadoopUtil.getStatusFromJobTracker(jobId)
                if (code != 0): 
                    print "ERROR: get status from job tracker, ErrorMsg [%s]" % err
                    return False, JOB_STATUS_UNKNOWN
                jobStatus = JOB_STATUS_RUNNING

        print "INFO: %s job status: %s" % (step, jobStatus)
        print "INFO: detail infomations: \n", detailInfo
        return True, jobStatus

    # for mapreduce 2.0
    def _translateJobStatus(self, mr2JobStatus):
        jobStatus = JOB_STATUS_UNKNOWN
        if mr2JobStatus == 'SUCCEEDED':
            jobStatus = JOB_STATUS_SUCCESS
        elif mr2JobStatus == 'RUNNING':
            jobStatus = JOB_STATUS_RUNNING
        elif mr2JobStatus == 'FAILED':
            jobStatus = JOB_STATUS_FAILED
        elif mr2JobStatus == 'ACCEPTED':
            jobStatus = JOB_STATUS_WAITING
        elif mr2JobStatus == 'KILLED':
            jobStatus = JOB_STATUS_FAILED
        else:
            jobStatus = JOB_STATUS_UNKNOWN
        return jobStatus

    # used by mapreduce 2.0
    def __doGetJobStatus2(self, showDetail, step, timeout):
        ret, jobId = self.__getJobInfo(step, timeout)
        if not ret:
            print "ERROR: get job info failed"
            return False, JOB_STATUS_UNKNOWN
        
        jobStatus = JOB_STATUS_UNKNOWN
        detailInfo = ""
        if jobId == "":
            jobStatus = JOB_STATUS_NOT_RUNNING
        else:
            print "INFO: %s jobid: %s" % (step, jobId)
            if not self.__isJobRunning(jobId, 5):
                mr2JobStatus, results = self.hadoopUtil.getApplicationInfo(jobId)
                jobStatus = self._translateJobStatus(mr2JobStatus)
                if jobStatus != JOB_STATUS_SUCCESS:
                    print "ERROR: job failed"
                    print '\n'.join(results)
                else:
                    print '\n'.join(results)
            else:
                detailInfo, err, code = self.hadoopUtil.getStatusFromJobTracker(jobId)
                if (code != 0):
                    print "ERROR: get status from job tracker, ErrorMsg [%s]" % err
                    return False, JOB_STATUS_UNKNOWN
                jobStatus = JOB_STATUS_RUNNING

        print "INFO: %s job status: %s" % (step, jobStatus)
        print "INFO: detail infomations: \n", detailInfo
        return True, jobStatus
    
    def isJobRunning(self):
        ret = self._checkJobRunning(JOB_BUILD_STEP)
        if ret != JOB_STATUS_NOT_RUNNING:
            return ret
        ret = self._checkJobRunning(JOB_MERGE_STEP)
        if ret != JOB_STATUS_NOT_RUNNING:
            return ret
        return self._checkJobRunning(JOB_END_MERGE_STEP)

    def _checkJobRunning(self, step):
        ret, jobId  = self.__getJobInfo(step, 5)
        if not ret:
            print "ERROR: get job info failed"
            return JOB_STATUS_UNKNOWN
        if jobId == "":
            return JOB_STATUS_NOT_RUNNING
        if self.__isJobRunning(jobId, 5):
            return JOB_STATUS_RUNNING
        else:
            return JOB_STATUS_NOT_RUNNING
        
    def __getJobInfo(self, step, timeout = 30):
        jobBaseDir = self.__getJobBaseDir(step)
        jobInfoFileName = jobBaseDir + self.JOB_INFO_FILE_NAME

        sleepTime = 2
        while timeout > 0:
            try:
                if not self.fsUtil.exists(jobInfoFileName):
                    print "WARN: job info file not exists, fileName [%s]" % \
                        jobInfoFileName
                    return True, ""

                content = self.fsUtil.cat(jobInfoFileName)
                if not content:
                    print "ERROR: failed to cat job info file, fileName [%s]" % \
                        jobInfoFileName
                    if timeout < sleepTime:
                        break
                    time.sleep(sleepTime)
                    timeout = timeout - sleepTime
                    continue
        
                lines = content.splitlines()
                if len(lines) == 1:
                    return True, lines[0]
                else:
                    print "ERROR: failed to get job info file[%s], "\
                        "content[%s], linesNum = %s" \
                        % (jobInfoFileName, content, len(lines))
                    return False, ""
            except Exception, e:
                print "WARN: get job info failed, catch exception: ", e

            if timeout < sleepTime:
                break
            time.sleep(sleepTime)
            timeout = timeout - sleepTime

        print "ERROR: get job info timeout"
        return False, ""

    def __isJobRunning(self, jobId, timeout = 30):
        sleepTime = 2
        while timeout > 0:
            try:
                jobIdList = self.hadoopUtil.getJobList()
                if jobId in jobIdList:
                    return True
                else:
                    return False
            except Exception, e:
                print "WARN: check job running failed, catch exception: ", e
                if timeout < sleepTime:
                    break
                time.sleep(sleepTime)
                timeout = timeout - sleepTime

        errorMsg = "ERROR: check job running failed"
        raise Exception, errorMsg

    def __getJobId(self, outFileName):
        ## return jobId
        maxLoopCount = 1800 # 15 minitues, because every loop tooks 0.5 second
        curLoopCount = 0;
        try:
            outFile = open(outFileName, "r+")
        except Exception, e:
            print "ERROR: failed to open outFile[%s], exception[%s]" \
                % (outFileName, str(e))
            return None

        jobId = None
        while True:
            if (curLoopCount >= maxLoopCount):
                print "ERROR: timeout while waiting job to start.."
                break
            curLoopCount += 1;

            time.sleep(0.5)

            outFile.seek(0)
            out = outFile.read()

            m = re.search(self.JOB_ID_START_TAG + "(.*?)\n", out)
            if m is not None:
                jobId = m.group(1)
                print "INFO: start job success."
                break

            if self.pipesProcess is None:
                print "ERROR: hadoop pipes process has not been started!!!"
                break

            if self.pipesProcess.poll() is not None:
                print "ERROR: hadoop pipes process has exited!!!"
                print "ERROR: error message is: ", out
                break
            
        outFile.close()
        return jobId

    def __writeJobInfo(self, jobId, jobName, jobInfoFileName):
        tmpJobInfoFile = self.TMP_DIR + jobName + "_job.info"
        tmpFile = open(tmpJobInfoFile, "w")
        content = "%(jobId)s\n" % {'jobId': jobId}
        tmpFile.write(content)
        tmpFile.close()
        
        if self.fsUtil.exists(jobInfoFileName):
            self.fsUtil.remove(jobInfoFileName)
                
        self.fsUtil.copy(tmpFile.name, jobInfoFileName)

    def __prepareNlineInputFile(self, mapInstanceCount, 
                               localTmpDir, hadoopTmpDir):
        #generate file in local
        nlineInputFileName = "nlineInput.txt"
        localNlineInputFilePath = os.path.join(localTmpDir, nlineInputFileName)
        hadoopNlineInputFilePath = os.path.join(hadoopTmpDir, nlineInputFileName)

        try:
            fd = open(localNlineInputFilePath, 'w')
            for line in range(0, int(mapInstanceCount)):
                fd.write(str(line) + '\n')
            fd.close()
        except Exception, e:
            raise Exception("ERROR: failed to write localNlineInputFile[%s]. exception[%s]" \
                % (localNlineInputFilePath, str(e)))

        #copy from local to hadoop
        self.fsUtil.copy(localNlineInputFilePath, hadoopNlineInputFilePath)
        return hadoopNlineInputFilePath

    # just for test
    def _setHadoopUtil(self, hadoopUtil):
        self.hadoopUtil = hadoopUtil
