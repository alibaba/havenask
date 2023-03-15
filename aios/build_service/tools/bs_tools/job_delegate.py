# -*- coding: utf-8 -*-

import os
import time
import re
import string
import process
from common_define import *
from range_util import RangeUtil
import json

class JobDelegate(object):
    def __init__(self, configPath, buildAppConf, buildRuleConf,
                 toolsConfig, fsUtil, clusterName):
        self.configPath = configPath
        self.buildAppConf = buildAppConf
        self.buildRuleConf = buildRuleConf
        self.toolsConfig = toolsConfig
        self.fsUtil = fsUtil
        self.generationId = 0
        self.clusterName = clusterName
        self.buildMode = None
        self.__initJobNames()
        self.buildPartFrom = None
        self.buildPartCount = None
        self.dataPath = None
        self.documentFormat = None
        self.rawDocSchemaName = None
        self.dataDescription = None
        self.readSrc = None
        self.mergeConfigName = ''
        self.parameters = ''
        self.logConfigPath = ''
        self.jobTimestamp = int(time.time())
        self.realtimeInfo = None

    def startJob(self, generationMeta, timeout, sleepInterval, buildMode, buildStep,
                 buildPartFrom, buildPartCount, mergeConfigName, dataPath=None, generationId=None,
                 documentFormat=None, rawDocSchemaName=None, dataDescription=None,
                 readSrc=None, parameters=None, logConfigPath='', realtimeInfo=None):
        '''
        public function for job command.
        no return value
        throw exception when failed.
        '''
        if buildPartFrom is None:
            self.buildPartFrom = 0
        else:
            self.buildPartFrom = buildPartFrom

        if buildPartCount is None:
            self.buildPartCount = self.buildRuleConf.partitionCount
        else:
            self.buildPartCount = buildPartCount

        self.buildMode = buildMode
        if self.buildMode == BUILD_MODE_INC:
            self.buildRuleConf.buildParallelNum = 1
            self.buildRuleConf.mergeParallelNum = 1
            self.buildRuleConf.partitionSplitNum = 1

        self.timeLeft = timeout
        self.sleepInterval = sleepInterval
        self.mergeConfigName = mergeConfigName
        self.dataPath = dataPath
        self.readSrc = readSrc
        self.documentFormat = documentFormat
        self.rawDocSchemaName = rawDocSchemaName
        self.dataDescription = dataDescription
        self.parameters = parameters
        if logConfigPath is not None:
            self.logConfigPath = logConfigPath

        if generationId is not None:
            self.generationId = generationId
        else:
            self.generationId = self.__initGenerationId(buildMode, buildStep)

        if buildMode == BUILD_MODE_FULL:
            self._checkGeneration(self.buildAppConf.indexRoot,
                                  self.buildRuleConf.partitionCount,
                                  self.buildPartFrom, self.buildPartCount,
                                  buildStep)

        self.__partitionSplitCheck()
        self.__initZookeeperLock()

        status =  self.isJobRunning()
        if status == JOB_STATUS_RUNNING:
            raise Exception("ERROR: job is already running")
        elif status == JOB_STATUS_UNKNOWN:
            raise Exception("ERROR: failed to check whether job is running or not.")

        self.__prepareTempIndexDir(buildMode, buildStep)
        self.__copyGenerationMeta(generationMeta, self.__getGenerationDir(self.indexRoot))
        self.__doStartJob(buildStep)

        self._renameTempIndexDir(buildMode, buildStep)
        self.__copyGenerationMetaToPartitions(generationMeta)
        self.__generateRealtimeInfoFile(realtimeInfo)

    def getJobStatus(self, buildStep, buildMode, showDetail, timeout = 60):
        '''
        public function for job command.
        return [True/False]
        print Job Status.
        not throw.
        '''
        assert(buildStep in [JOB_BOTH_STEP, JOB_BUILD_STEP, JOB_MERGE_STEP, JOB_END_MERGE_STEP])
        assert(buildMode in [BUILD_MODE_FULL, BUILD_MODE_INC])
        if buildStep in [JOB_BUILD_STEP, JOB_BOTH_STEP]:
            print "INFO: begin get build job status"
            hasBuild = True
            ret, jobStatus = self.doGetJobStatus(
                showDetail, JOB_BUILD_STEP, timeout)
            if (not ret) or jobStatus != JOB_STATUS_SUCCESS:
                return ret

        if buildMode == BUILD_MODE_INC:
            return True

        if buildStep in [JOB_MERGE_STEP, JOB_BOTH_STEP]:
            print "INFO: begin get merge job status"
            ret, jobStatus = self.doGetJobStatus(
                showDetail, JOB_MERGE_STEP, timeout)
            if (not ret) or jobStatus != JOB_STATUS_SUCCESS:
                return ret

        if buildStep in [JOB_END_MERGE_STEP, JOB_BOTH_STEP]:
            print "INFO: begin get end merge job status"
            ret, jobStatus = self.doGetJobStatus(
                showDetail, JOB_END_MERGE_STEP, timeout)
            if (not ret) or jobStatus != JOB_STATUS_SUCCESS:
                return ret
        return True

    def stopJob(self):
        '''
        public function for job command
        child class will inherit it.
        return [True/False]
        not throw.
        '''
        assert(False)
        return False

    # copy to generation dir for build plugin
    def __copyGenerationMeta(self, generationMeta, generationDir):
        destGeneratorMeta = generationDir + "/generation_meta"
        if generationMeta and not self.fsUtil.copy(generationMeta, destGeneratorMeta):
            print "ERROR: failed to copy generationMeta to %s" % generationDir
            return False
        return True

    def __copyGenerationMetaToPartitions(self, generationMeta):
        '''
        private function : copy generation meta to each partition dir.
        throw exception when copy failed.
        '''
        if not generationMeta:
            return
        finalIndexGenerationDir = self.__getGenerationDir(self.buildAppConf.indexRoot)
        fileList = self.fsUtil.listDir(finalIndexGenerationDir)
        pattern = "partition_\d+_\d+"
        for f in fileList:
            if not re.match(pattern, f):
                continue
            indexPartitionDir = finalIndexGenerationDir + "/" + f
            destGeneratorMeta = indexPartitionDir + "/generation_meta"
            if self.fsUtil.exists(destGeneratorMeta):
                continue
            self.fsUtil.copy(generationMeta, destGeneratorMeta)

    def __generateRealtimeInfoFile(self, realtimeInfo=None):
        '''
        private function : generate realtime_info.json file.
        '''
        if not realtimeInfo:
            return
        finalIndexGenerationDir = self.__getGenerationDir(self.buildAppConf.indexRoot)
        realtimeInfoFilePath = os.path.join(finalIndexGenerationDir, "realtime_info.json")
        if self.fsUtil.exists(realtimeInfoFilePath):
            return
        realtimeInfo = json.loads(realtimeInfo)
        with open(realtimeInfoFilePath, 'w') as f:
            json.dump(realtimeInfo, f, indent=4)

    def __getTempGenerationDir(self):
        return "%s/__temp__generation_%d/" % (os.path.join(
                self.buildAppConf.indexRoot, self.clusterName), self.generationId)

    def __getGenerationDir(self, indexDir):
        return "%s/generation_%d/" % (os.path.join(indexDir, self.clusterName),
                                      self.generationId)

    def __prepareTempIndexDir(self, buildMode, buildStep):
        '''
        private function: create temp dir for full build[not (inc build) or merge ].
        throw exception when operate dir failed.
        '''
        if not self.__needFullBuildJob(buildMode, buildStep):
            self.indexRoot = self.buildAppConf.indexRoot
            return

        self.indexRoot = self.__getTempGenerationDir()
        if self.fsUtil.exists(self.indexRoot):
            self.fsUtil.remove(self.indexRoot)
        self.fsUtil.mkdir(self.indexRoot, True)

    def _renameTempIndexDir(self, buildMode, buildStep):
        '''
        private function: rename full build temp dir to official dir.
        throw exception when operate dir failed.
        '''
        if not self.__needFullBuildJob(buildMode, buildStep):
            return

        finalIndexGenerationDir = self.__getGenerationDir(self.buildAppConf.indexRoot)
        tempIndexGenerationDir = self.__getGenerationDir(self.indexRoot)
        partitionMsg = {}
        partitionMsg["partition_count"] = self.buildRuleConf.partitionCount
        partitionCountStr = json.dumps(partitionMsg)
        f = open('partition_count', 'w')
        f.write(partitionCountStr)
        f.close()
        self.fsUtil.copy('./partition_count', tempIndexGenerationDir + '/partition_count')
        self.fsUtil.remove('./partition_count')
        if not self.fsUtil.exists(finalIndexGenerationDir):
            self.fsUtil.rename(tempIndexGenerationDir, finalIndexGenerationDir)
            self.fsUtil.remove(self.indexRoot)
            return

        # mv temp partition to final index root
        pattern = "partition_\d+_\d+"
        fileList = self.fsUtil.listDir(tempIndexGenerationDir)
        for f in fileList:
            if not re.match(pattern, f):
                continue
            src = tempIndexGenerationDir
            dst = finalIndexGenerationDir
            self.fsUtil.rename(src + '/' + f, dst + '/' + f)

        self.fsUtil.remove(self.indexRoot)

    def __getBuildInstanceCount(self):
        needReduce = self.__needReduce()
        if needReduce:
            reduceInstanceCount = self.buildPartCount * self.buildRuleConf.buildParallelNum
            # start all map to read all data for partition
            mapInstanceCount = self.buildRuleConf.partitionCount * \
                self.buildRuleConf.buildParallelNum * self.buildRuleConf.mapReduceRatio
        else:
            reduceInstanceCount = 0
            mapInstanceCount = self.buildPartCount * self.buildRuleConf.buildParallelNum
        return mapInstanceCount, reduceInstanceCount

    def __doStartJob(self, buildStep):
        '''
        private function: start and finish all jobs.
        throw exception when run job failed.
        '''
        if buildStep in [ JOB_BUILD_STEP, JOB_BOTH_STEP ]:
            self.__startBuildJob()

        if buildStep in [ JOB_MERGE_STEP, JOB_BOTH_STEP ]:
            self.__startMergeJob()

        if buildStep in [ JOB_END_MERGE_STEP, JOB_BOTH_STEP ]:
            self.__startEndMergeJob()

    def __startBuildJob(self):
        mapInstanceCount, reduceInstanceCount = self.__getBuildInstanceCount()
        self.__runOneJob(JOB_BUILD_STEP,
                         mapInstanceCount, reduceInstanceCount)

    def __startMergeJob(self):
        mapInstanceCount = self.__getMergePartitionCount()
        reduceInstanceCount = self.__getMergePartitionCount() * self.buildRuleConf.mergeParallelNum
        self.__runOneJob(JOB_MERGE_STEP, mapInstanceCount, reduceInstanceCount)

    def __startEndMergeJob(self):
        mapInstanceCount = self.__getMergePartitionCount()
        reduceInstanceCount = 1
        self.__runOneJob(JOB_END_MERGE_STEP, mapInstanceCount, reduceInstanceCount)
        self.__partitionSplitMerge()

    def __getMergePartitionCount(self):
        return self.buildPartCount * self.buildRuleConf.partitionSplitNum

    def __needFullBuildJob(self, buildMode, buildStep):
        return buildMode == BUILD_MODE_FULL and buildStep in [JOB_BUILD_STEP, JOB_BOTH_STEP]

    def __runOneJob(self, buildStep, mapCount, reduceCount):
        print "INFO: start %s job... "  % buildStep

        beginTime = time.time()
        self.doStartJob(buildStep, mapCount, reduceCount)
        self.waitJobFinish(self.timeLeft, buildStep, self.sleepInterval)
        endTime = time.time()
        self.timeLeft -= endTime - beginTime

        if not self.__isJobSuccessful(buildStep):
            raise Exception("run %s job fail!" % buildStep)

    def __isJobSuccessful(self, step):
        ret, jobStatus = self.doGetJobStatus(False, step, 60)
        if not ret:
            errorMsg = "get job status failed"
            raise Exception(Exception, errorMsg)

        if jobStatus == JOB_STATUS_SUCCESS:
            return True
        else:
            print "ERROR: job isn't sucessfull, job status [%s]" % jobStatus
            return False

    def __needReduce(self):
        return self.buildRuleConf.needPartition

    def __initGenerationId(self, buildMode, buildStep):
        p = process.Process()
        cmd = self.toolsConfig.getGenerationScanner() + \
            ' %s %s' % (self.buildAppConf.indexRoot, self.clusterName)
        data, error, code = p.run(cmd)
        if code != 0:
            raise Exception("scan geneartion failed data[%s] error[%s] code[%s]"
                            % (data, error, str(code)))
        gid = int(data)

        if self.__needFullBuildJob(buildMode, buildStep):
            gid += 1

        if gid < 0:
            raise Exception("invalid generation[%d] for buildMode[%s] and buildStep[%s]" %
                            (gid, buildMode, buildStep))
        return gid

    def __initZookeeperLock(self):
        if not self.buildAppConf.zookeeperRoot:
            print 'WARN: run without zookeeper lock'
            return
        if not self.fsUtil.exists(self.buildAppConf.zookeeperRoot):
            self.fsUtil.mkdir(self.buildAppConf.zookeeperRoot, True)

    def __initJobNames(self):
        jobPrefix = self.__getJobPrefix()
        self.buildJobName = "%s-%s-%s" % (jobPrefix,
                                          self.clusterName,
                                          JOB_BUILD_STEP)
        self.mergeJobName = "%s-%s-%s" % (jobPrefix,
                                          self.clusterName,
                                          JOB_MERGE_STEP)
        self.endMergeJobName = "%s-%s-%s" % (jobPrefix,
                                             self.clusterName,
                                             JOB_END_MERGE_STEP)

    def __getJobPrefix(self):
        regularExpr = ".*://(.*?)/(.*)"
        m = re.match(r"" + regularExpr, self.buildAppConf.indexRoot)

        if m == None:
            return self.buildAppConf.indexRoot.replace("/", "_")
        return m.group(2).replace("/", "_")

    def generateJobParamStr(self):
        '''
        for child class: create job param map, convert it to string.
        return job param string.
        always success
        '''
        jobParamDict = {}
        jobParamDict[BUILD_MODE] = self.buildMode
        jobParamDict[CONFIG_PATH] = self.configPath
        if self.dataPath:
            jobParamDict[DATA_PATH] = self.dataPath
        if self.documentFormat:
            jobParamDict[DOCUMENT_FORMAT] = self.documentFormat
        if self.rawDocSchemaName:
            jobParamDict[RAW_DOC_SCHEMA_NAME] = self.rawDocSchemaName
        if self.dataDescription:
            jobParamDict[DATA_DESCRIPTION] = self.dataDescription
        if self.readSrc:
            jobParamDict[READ_SRC] = self.readSrc
        jobParamDict[GENERATION] = str(self.generationId)
        jobParamDict[CLUSTER_NAME] = str(self.clusterName)
        jobParamDict[SRC_SIGNATURE] = str(int(time.time()))
        jobParamDict[MERGE_TIMESTAMP] = str(self.jobTimestamp)
        jobParamDict[MERGE_CONFIG_NAME] = self.mergeConfigName

        # job doesn't read config, get all from job params
        jobParamDict[BUILD_RULE_PARTITION_COUNT] = str(self.buildRuleConf.partitionCount)
        jobParamDict[BUILD_RULE_BUILD_PARALLEL_NUM] = str(self.buildRuleConf.buildParallelNum)
        jobParamDict[BUILD_RULE_PARTITION_SPLIT_NUM] = str(self.buildRuleConf.partitionSplitNum)
        jobParamDict[BUILD_RULE_MERGE_PARALLEL_NUM] = str(self.buildRuleConf.mergeParallelNum)
        jobParamDict[BUILD_RULE_MAP_REDUCE_RATIO] = str(self.buildRuleConf.mapReduceRatio)
        jobParamDict[BUILD_RULE_NEED_PARTITION] = str(self.buildRuleConf.needPartition)
        jobParamDict[BUILD_APP_ZOOKEEPER_ROOT] = self.buildAppConf.zookeeperRoot
        jobParamDict[BUILD_APP_AMONITOR_PORT] = self.buildAppConf.amonitorPort
        jobParamDict[BUILD_APP_USER_NAME] = self.buildAppConf.userName
        jobParamDict[BUILD_APP_SERVICE_NAME] = self.buildAppConf.serviceName

        # TODO : full build use temp dir.
        jobParamDict[INDEX_ROOT_PATH] = self.indexRoot
        if self.buildPartFrom is not None:
            jobParamDict[BUILD_PART_FROM] = str(self.buildPartFrom)
        if self.buildPartCount is not None:
            jobParamDict[BUILD_PART_COUNT] = str(self.buildPartCount)
        jobParamStr = json.dumps(jobParamDict)
        return jobParamStr

    def replaceTemplate(self, templateName, fileName, paramDict):
        '''
        for child class: replace job description file with param dict.
        write target file to '$fileName'
        return [True/False].
        '''
        configTemplateFile = os.path.join(self.configPath,
                                          'start_config_template/%s.template' % templateName)
        if not self.fsUtil.exists(configTemplateFile):
            curPath = os.path.split(os.path.realpath(__file__))[0]
            configTemplateFile = '%s/default_%s.template' % (curPath, templateName)

        templateStr = self.fsUtil.cat(configTemplateFile)

        try:
            templateStr = templateStr % paramDict
        except Exception, e:
            print "ERROR: make job config failed, " \
                "some configurations may be wrong, catch exception:", e
            print "ERROR: template file content is: %s, paramDict is: %s" % (
                templateStr, str(paramDict))
            return False

        try:
            startJobConfigFile = open(fileName, 'w')
            startJobConfigFile.write(templateStr)
            startJobConfigFile.flush()
            startJobConfigFile.close()
        except Exception, e:
            print "ERROR: failed to write startJobConfigString to file[%s]" \
                % fileName
            print "       exception:", e
            return False

        return True

    def _checkGeneration(self, indexRoot, partitionCount,
                         buildPartFrom, buildPartCount, buildStep):
        generationDir = self.__getGenerationDir(indexRoot)
        if not self.fsUtil.exists(generationDir):
            return
        # read partition_count
        partitionCountFile = generationDir + '/partition_count'
        if not self.fsUtil.exists(partitionCountFile):
            print "partitionCountFile[%s] does not exist, not check" % partitionCountFile
            return
        content = self.fsUtil.cat(partitionCountFile)
        jsonMap = json.loads(content)
        originalPartCount = int(jsonMap['partition_count'])
        if originalPartCount != partitionCount:
            raise Exception('inconsistent partition_count, raw[%d], current[%d]'
                            % (originalPartCount, partitionCount))
        self._checkPartitionFromAndCount(partitionCount, buildPartFrom,
                                         buildPartCount, generationDir,
                                         buildStep)

    def _checkPartitionFromAndCount(self, partitionCount, buildPartFrom,
                                    buildPartCount, generationDir, buildStep):
        allRanges = RangeUtil.splitRange(partitionCount)
        toBuildRanges = allRanges[buildPartFrom:(buildPartFrom+buildPartCount)]
        fileList = self.fsUtil.listDir(generationDir)
        pattern = "partition_\d+_\d+"
        for f in fileList:
            if not re.match(pattern, f):
                continue
            strs = f.split('_')
            rangeFrom = int(strs[1])
            rangeTo = int(strs[2])
            for toBuildRange in toBuildRanges:
                if (rangeFrom, rangeTo) == toBuildRange and buildStep == JOB_BUILD_STEP:
                    raise Exception('%s has already been built, can not be built twice' % f)

    def __partitionSplitCheck(self):
        if self.buildRuleConf.partitionSplitNum == 1:
            return
        cmd = self.toolsConfig.getPartitionSplitMerger()
        cmd += ' check %s %s ' % (self.configPath, self.clusterName)
        p = process.Process()
        data, error, code = p.run(cmd)
        if code != 0:
            raise Exception("partition check failed, data[%s] error[%s] code[%s]"
                            % (data, error, str(code)))

    def __partitionSplitMerge(self):
        if self.buildRuleConf.partitionSplitNum == 1:
            return
        cmd = self.toolsConfig.getPartitionSplitMerger()
        cmd = '%(cmd)s merge \'%(generationDir)s\' %(partFrom)d %(buildPartCount)d %(globalPartCount)d %(splitNum)d %(parallelNum)d' % \
        { 'cmd' : cmd,
          'generationDir' : self.__getGenerationDir(self.indexRoot),
          'partFrom' : self.buildPartFrom,
          'buildPartCount' : self.buildPartCount,
          'globalPartCount' : self.buildRuleConf.partitionCount,
          'splitNum' : self.buildRuleConf.partitionSplitNum,
          'parallelNum' : self.buildRuleConf.buildParallelNum / self.buildRuleConf.partitionSplitNum }

        p = process.Process()
        data, error, code = p.run(cmd)
        if code != 0:
            raise Exception("partition check failed, data[%s] error[%s] code[%s]"
                            % (data, error, str(code)))

    def time2str(self, value):
        floatValue = string.atof(value)
        if floatValue == -1:
            return value
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(floatValue))

    # function for sub class
    # return (True/False, JOB_STATUS)
    # JOB_STATUS in [JOB_STATUS_FAILED, JOB_STATUS_SUCCESS ...]
    def doGetJobStatus(self, showDetail, step, timeout):
        assert(False)
        return False

    # return JOB_STATUS
    def isJobRunning(self):
        assert(False)
        return JOB_STATUS_UNKNOWN

    # return True/False
    def waitJobFinish(self, timeout, step, sleepInterval):
        assert(False)
        return False
