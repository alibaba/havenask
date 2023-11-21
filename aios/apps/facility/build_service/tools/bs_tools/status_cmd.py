# -*- coding: utf-8 -*-

from admin_cmd_base import AdminCmdBase
from build_service.proto.Admin_pb2 import *
from build_service.proto.BasicDefs_pb2 import *
from build_service.proto.Heartbeat_pb2 import *
from hippo_py_sdk.hippo_client import HippoClient
from hippo.proto.Common_pb2 import *
import include.json_wrapper as json
import common_define
import datetime
import admin_locator
import build_app_config


class GetStatusCmd(AdminCmdBase):
    '''
    bs gs -c configPath
    bs gs -c configPath -t summary -g generationId
    bs gs -c configPath -t detail
    bs gs -c configPath -t detail -g generationId
    bs gs -c configPath -t error

options:
    -c configPath               : required, configPath for build_service
    -a app_name                  : optional, app name to build
    -t commandType              : optional, command type (default: summary)
       summary:          service summary
       detail:           detail information about service
       error:            error information about service
       worker:           worker information about service
       counter:          counter information of active worker nodes, to also display counters of finished worker nodes, add --all option.
    -g generationId             : optional, generationIds, if not set, print all generations
    --history                   : optional, get history generation infos

examples:
    bs gs -c config_path
    bs gs -c config_path --history
    bs gs -c config_path -t summary -g 0,1,2
    bs gs -c config_path -t detail -g 1,2
    bs gs -c config_path -t source
    '''
    ADMIN_GET_SERVICE_INFO_METHOD = 'getServiceInfo'

    def __init__(self):
        super(GetStatusCmd, self).__init__()
        self.generationIds = []
        self.showAllCounters = False

    def addOptions(self):
        super(GetStatusCmd, self).addOptions()
        self.parser.add_option('-z', '--zookeeper', action='store', dest='zkRoot')
        self.parser.add_option('-t', '--type', type='choice', action='store',
                               dest='commandType', default='summary',
                               choices=['summary', 'detail', 'error', 'source', 'worker', 'counter'])
        self.parser.add_option('-g', '--generation', action='store', dest='generation', default='')
        self.parser.add_option('', '--history', action='store_true', dest='history')
        self.parser.add_option('', '--all', action='store_true', dest='allcounter')

    def initMember(self, options):
        super(GetStatusCmd, self).initMember(options)
        self.zkRoot = options.zkRoot
        self.commandType = options.commandType
        self.history = options.history
        self.showAllCounters = options.allcounter
        if len(options.generation) > 0:
            self.generationIds = [int(gid) for gid in options.generation.split(',')]

    def checkOptionsValidity(self, options):
        if not options.configPath and not options.zkRoot:
            raise Exception('-c config_path or -z zookeeper_root is needed.')

    def createRequest(self):
        request = ServiceInfoRequest()
        if self.history:
            request.getHistory = True
        if self.appName != '':
            request.appName = self.appName
        if len(self.generationIds) == 1:
            request.buildId.generationId = self.generationIds[0]
        if self.commandType == 'detail':
            request.fillEffectiveIndexInfo = True
        return request

    def run(self):
        response = self.call(self.ADMIN_GET_SERVICE_INFO_METHOD)
        if 'source' == self.commandType:
            print self.processSourceStatus(response)
        elif 'error' == self.commandType:
            print self.processErrorStatus(response)
        elif 'worker' == self.commandType:
            print self.processWorkerStatus(response)
        elif 'counter' == self.commandType:
            print self.processCounterStatus(response)
        else:
            print self.processResponse(response, self.commandType)

    def processSourceStatus(self, response):
        result = ''
        for generationInfo in response.generationInfos:
            dataDescriptions = json.read(str(generationInfo.dataDescriptions))
            for dataDescription in dataDescriptions:
                readerSrc = str(dataDescription[common_define.READ_SRC])
                for k, v in dataDescription.items():
                    outK = '%s.%s.%s.%s.%s' % (str(generationInfo.buildId.appName),
                                               str(generationInfo.buildId.dataTable),
                                               str(generationInfo.buildId.generationId),
                                               readerSrc,
                                               str(k))
                    outV = v
                    result += '%s: %s\n' % (outK, outV)
        return result

    def processResponse(self, response, commandType):
        result = ''
        result += 'AdminAdress: ' + response.adminAddress + '\n'
        result += 'AdminConfig: ' + response.adminConfig + '\n'
        result += 'Worker(expected): %d' % self.getGlobalWorkerCount(response) + '\n'
        for generationInfo in response.generationInfos:
            buildId = generationInfo.buildId
            if not self.needPrintGeneration(buildId.generationId):
                continue
            result += self.processGenerationStatus(generationInfo, commandType)
        return result

    def getGlobalWorkerCount(self, response):
        expectedCount = 0
        for generationInfo in response.generationInfos:
            buildId = generationInfo.buildId
            if not self.needPrintGeneration(buildId.generationId):
                continue
            for processorInfo in generationInfo.processorInfos:
                expectedCount += self.getProcessorExpectWorkerCount(processorInfo)
            for clusterInfo in generationInfo.buildInfo.clusterInfos:
                expectedCount += self.getClusterExpectWorkerCount(clusterInfo)
        return expectedCount

    def addTuple(self, left, right):
        first = left[0] + right[0]
        second = left[1] + right[1]
        return (first, second)

    def processProcessorErrorStatus(self, prefix, partitionInfo):
        result = ''
        processorClusterPrefix = '%s.Processor[%s]' % \
                                 (prefix, self.getRangeStr(partitionInfo.pid.range))
        for errorInfo in partitionInfo.currentStatus.errorInfos:
            errorMsg = "%s.[%s].[%s].[%s]" % (
                processorClusterPrefix, partitionInfo.currentStatus.longAddress.ip,
                self.timestampToDatetime(errorInfo.errorTime),
                errorInfo.errorMsg)
            result += errorMsg + '\n'
        return result

    def processClusterCounterStatus(self, prefix, clusterInfo):
        result = ''
        clusterName = clusterInfo.clusterName
        clusterPrefix = '%s.%s' % (prefix, clusterName)
        rolePrefix = None
        if len(clusterInfo.mergerInfo.partitionInfos) != 0:
            rolePrefix = '%s.%s' % (clusterPrefix, 'Merger')
            workerInfo = clusterInfo.mergerInfo
            workerType = 'merger'
        else:
            rolePrefix = "%s.%s" % (clusterPrefix, 'Builder')
            workerInfo = clusterInfo.builderInfo
            workerType = 'builder'

        for partitionInfo in workerInfo.partitionInfos:
            result += self.processPartitionCounters(rolePrefix, partitionInfo)
        return result

    def processClusterErrorStatus(self, prefix, clusterInfo):
        result = ''
        clusterName = clusterInfo.clusterName
        clusterPrefix = '%s.%s' % (prefix, clusterName)
        rolePrefix = None
        if len(clusterInfo.mergerInfo.partitionInfos) != 0:
            rolePrefix = '%s.%s' % (clusterPrefix, 'Merger')
            workerInfo = clusterInfo.mergerInfo
            workerType = 'merger'
        else:
            rolePrefix = "%s.%s" % (clusterPrefix, 'Builder')
            workerInfo = clusterInfo.builderInfo
            workerType = 'builder'

        for partitionInfo in workerInfo.partitionInfos:
            partitionInfoPrefix = '%s[%s]' % \
                                  (rolePrefix, self.getRangeStr(partitionInfo.pid.range))
            for errorInfo in partitionInfo.currentStatus.errorInfos:
                errorMsg = "%s[%s].[%s].[%s].[%s]" % (
                    rolePrefix, self.getRangeStr(partitionInfo.pid.range),
                    partitionInfo.currentStatus.longAddress.ip,
                    self.timestampToDatetime(errorInfo.errorTime),
                    errorInfo.errorMsg)
                result += errorMsg + '\n'
        return result

    def processErrorStatus(self, response):
        result = ''
        for generationInfo in response.generationInfos:
            prefix = '%s.%s.%d' % (
                generationInfo.buildId.appName,
                generationInfo.buildId.dataTable,
                generationInfo.buildId.generationId)
            for processorInfo in generationInfo.processorInfos:
                for partitionInfo in processorInfo.partitionInfos:
                    result += self.processProcessorErrorStatus(prefix, partitionInfo)
            for clusterInfo in generationInfo.buildInfo.clusterInfos:
                result += self.processClusterErrorStatus(prefix, clusterInfo)

        return result

    def processPartitionCounters(self, prefix, partitionInfo):
        result = ''
        prefix = '%s[%s]' % (prefix, self.getRangeStr(partitionInfo.pid.range))
        for counterItem in partitionInfo.counterItems:
            counterMsg = "\t%s : %d\n" % (counterItem.name, counterItem.value)
            result += counterMsg
        if len(result) > 0:
            return "%s\n%s" % (prefix, result)
        return result

    def processGenerationCounters(self, prefix, generationInfo):
        result = ''
        for roleCounterInfo in generationInfo.roleCounterInfo:
            rolePrefix = "%s:" % (roleCounterInfo.roleName)
            roleCounters = ''
            for counterItem in roleCounterInfo.counterItem:
                counterMsg = "\t%s : %d\n" % (counterItem.name, counterItem.value)
                roleCounters += counterMsg
            if len(roleCounters) > 0:
                result += "%s\n%s" % (rolePrefix, roleCounters)
        return result

    def processCounterStatus(self, response):
        result = ''
        for generationInfo in response.generationInfos:
            prefix = '%s.%s.%d' % (
                generationInfo.buildId.appName,
                generationInfo.buildId.dataTable,
                generationInfo.buildId.generationId)

            if self.showAllCounters:
                result += self.processGenerationCounters(prefix, generationInfo)
                continue

            for processorInfo in generationInfo.processorInfos:
                for partitionInfo in processorInfo.partitionInfos:
                    result += self.processPartitionCounters("%s.Processor" % (prefix), partitionInfo)

            for clusterInfo in generationInfo.buildInfo.clusterInfos:
                result += self.processClusterCounterStatus(prefix, clusterInfo)

            for jobPartitionInfo in generationInfo.jobInfo.partitionInfos:
                result += self.processPartitionCounters("%s.Job" % (prefix), jobPartitionInfo)
        return result

    def processGenerationStatus(self, generationInfo, commandType):
        result = ''
        buildId = generationInfo.buildId
        prefix = '%s.%s.%d' % (buildId.appName, buildId.dataTable, buildId.generationId)
        result += '%s.BuildStep: %s\n' % (prefix, generationInfo.buildStep)
        result += '%s.ConfigPath: %s\n' % (prefix, generationInfo.configPath)
        if len(generationInfo.processorInfos) != 0:
            for processorInfo in generationInfo.processorInfos:
                result += self.printProcessorStatus(prefix, processorInfo, commandType,
                                                    len(generationInfo.processorInfos))
        if generationInfo.HasField('buildInfo'):
            result += self.printBuilderStatus(prefix, generationInfo.buildInfo, commandType)
        if generationInfo.HasField('jobInfo'):
            result += self.printJobStatus(prefix, generationInfo.jobInfo, commandType)
        return result

    def printJobStatus(self, prefix, jobInfo, commandType):
        result = ''
        clusterName = jobInfo.clusterName
        clusterPrefix = '%s.%s' % (prefix, clusterName)
        result += '%s.Job.PartitionCount: %d\n' % (clusterPrefix, jobInfo.partitionCount)
        result += '%s.Job.buildParallelNum: %d\n' % (clusterPrefix, jobInfo.buildParallelNum)
        result += '%s.Job.dataDescriptionId: %d\n' % (clusterPrefix, jobInfo.dataDescriptionId)
        result += '%s.Job.mergeConfigName: %s\n' % (clusterPrefix, jobInfo.mergeConfigName)
        dataSourceStr = ''
        try:
            dataSourceMap = json.read(str(jobInfo.dataDescription))  # jobInfo.dataDescription is typeof unicode
            for k, v in dataSourceMap.items():
                dataSourceStr += '%s=%s;' % (k, v)
        except Exception as e:
            pass
        result += '%s.Job.DataSource: %s\n' % (clusterPrefix, dataSourceStr)

        return result

    def printProcessorStatus(self, prefix, processorInfo, commandType, processorTaskCount):
        result = ''
        taskId = ''
        if len(processorInfo.partitionInfos) > 0:
            if processorInfo.partitionInfos[0].pid.taskId != '0':
                taskId = processorInfo.partitionInfos[0].pid.taskId + '.'
        result += '%s.Processor.%sWorker(expected): %d\n' %\
            (prefix, taskId, self.getProcessorExpectWorkerCount(processorInfo))
        dataSourceStr = ''
        try:
            # processorInfo.dataDescription is typeof unicode
            dataSourceMap = json.read(str(processorInfo.dataDescription))
            for k, v in dataSourceMap.items():
                dataSourceStr += '%s=%s;' % (k, v)
        except Exception as e:
            pass
        result += '%s.Processor.%sDataSource: %s\n' % (prefix, taskId, dataSourceStr)
        result += '%s.Processor.%sProcessorStep: %s\n' %\
            (prefix, taskId, processorInfo.processorStep)
        if commandType == 'detail':
            # print detail info for each partition
            for partitionInfo in processorInfo.partitionInfos:
                processorClusterPrefix = '%s.Processor.%sRange[%s]' % \
                    (prefix, taskId, self.getRangeStr(partitionInfo.pid.range))
                result += processorClusterPrefix + '.Address: ' +\
                    partitionInfo.currentStatus.longAddress.ip + '\n'
                currentConfig, targetConfig = self.getConfigPath(partitionInfo)
                result += processorClusterPrefix + '.Config.Current: %s\n' % currentConfig
                result += processorClusterPrefix + '.Config.Target: %s\n' % targetConfig
                result += processorClusterPrefix + '.StartLocator.Target: (%s, %s)\n' %\
                    (str(partitionInfo.targetStatus.startLocator.sourceSignature),
                     str(partitionInfo.targetStatus.startLocator.checkpoint))
                result += processorClusterPrefix + '.StartLocator.Current: (%s, %s)\n' % \
                    (str(partitionInfo.currentStatus.targetStatus.startLocator.sourceSignature),
                     str(partitionInfo.currentStatus.targetStatus.startLocator.checkpoint))
                result += processorClusterPrefix + '.StopTimestamp.Target: %s\n' %\
                    str(partitionInfo.targetStatus.stopTimestamp)
                result += processorClusterPrefix + '.StopTimestamp.Current: %s\n' %\
                    str(partitionInfo.currentStatus.targetStatus.stopTimestamp)
                result += processorClusterPrefix + '.Status: %s\n' % (
                    self.getWorkerStatusStr(partitionInfo.currentStatus.status))
                result += processorClusterPrefix + '.StopLocator: (%s, %s)\n' % \
                    (str(partitionInfo.currentStatus.stopLocator.sourceSignature),
                     str(partitionInfo.currentStatus.stopLocator.checkpoint))
        return result

    def getWorkerStatusStr(self, status):
        if status == WS_UNKNOWN:
            return "WS_UNKNOWN"
        elif status == WS_STARTING:
            return "WS_STARTING"
        elif status == WS_STARTED:
            return "WS_STARTED"
        elif status == WS_STOPPED:
            return "WS_STOPPED"
        elif status == WS_SUSPEND_READ:
            return "WS_SUSPEND_READ"
        else:
            assert False, "unknown status %s" % str(status)

    def printBuilderStatus(self, prefix, buildInfo, commandType):
        result = ''
        for clusterInfo in buildInfo.clusterInfos:
            clusterName = clusterInfo.clusterName
            clusterPrefix = '%s.%s' % (prefix, clusterName)
            rolePrefix = None
            if len(clusterInfo.mergerInfo.partitionInfos) != 0:
                rolePrefix = '%s.%s' % (clusterPrefix, 'Merger')
                workerInfo = clusterInfo.mergerInfo
                workerType = 'merger'
            elif len(clusterInfo.taskInfo.partitionInfos) != 0:
                rolePrefix = "%s.%s" % (clusterPrefix, 'Task')
                workerInfo = clusterInfo.taskInfo
                workerType = 'task'
            else:
                rolePrefix = "%s.%s" % (clusterPrefix, 'Builder')
                workerInfo = clusterInfo.builderInfo
                workerType = 'builder'

            result += '%s.ClusterStep: %s\n' % \
                (clusterPrefix, clusterInfo.clusterStep)
            result += '%s.LastVersionTimestamp: %s\n' % (
                clusterPrefix, str(clusterInfo.lastVersionTimestamp))
            result += '%s.PendingMergeTasks: %s\n' % (
                clusterPrefix, self.getPendingMergeTasks(clusterInfo))
            result += '%s.Worker(expected): %d\n' % (rolePrefix, self.getClusterExpectWorkerCount(clusterInfo))

            if commandType == 'detail':
                effectiveIndexInfo = clusterInfo.lastVersionEffectiveIndexInfo.replace('\n', '').replace(' ', '')
                result += '%s.EffectiveIndexInfo: %s\n' % (clusterPrefix, effectiveIndexInfo)

                for partitionInfo in workerInfo.partitionInfos:
                    partitionInfoPrefix = '%s.Range[%s]' % (rolePrefix, self.getRangeStr(partitionInfo.pid.range))
                    result += "%s.Address.%s\n" % (partitionInfoPrefix, partitionInfo.currentStatus.longAddress.ip)
                    if workerType == "task":
                        currentConfig, targetConfig = self.getConfigPath(partitionInfo, True)
                    else:
                        currentConfig, targetConfig = self.getConfigPath(partitionInfo)
                    result += '%s.Config.Current: %s\n' % (partitionInfoPrefix, currentConfig)
                    result += '%s.Config.Target: %s\n' % (partitionInfoPrefix, targetConfig)
                    if workerType == 'builder':
                        result += '%s.StartTimestamp.Current: %s\n' % (
                            partitionInfoPrefix, partitionInfo.currentStatus.targetStatus.startTimestamp)
                        result += '%s.StartTimestamp.Target: %s\n' % (
                            partitionInfoPrefix, partitionInfo.targetStatus.startTimestamp)
                        result += '%s.StopTimestamp.Current: %s\n' % (
                            partitionInfoPrefix, partitionInfo.currentStatus.targetStatus.stopTimestamp)
                        result += '%s.StopTimestamp.Target: %s\n' % (
                            partitionInfoPrefix, partitionInfo.targetStatus.stopTimestamp)
                        result += partitionInfoPrefix + '.Status: %s\n' % (
                            self.getWorkerStatusStr(partitionInfo.currentStatus.status))
                    if workerType == 'merger':
                        result += '%s.MergeConfigName.Current: %s\n' % (
                            partitionInfoPrefix, partitionInfo.currentStatus.targetStatus.mergeTask.mergeConfigName)
                        result += '%s.MergeConfigName.Target: %s\n' % (
                            partitionInfoPrefix, partitionInfo.targetStatus.mergeTask.mergeConfigName)
                        result += '%s.Timestamp.Current: %s\n' % (
                            partitionInfoPrefix, partitionInfo.currentStatus.targetStatus.mergeTask.timestamp)
                        result += '%s.Timestamp.Target: %s\n' % (
                            partitionInfoPrefix, partitionInfo.targetStatus.mergeTask.timestamp)
                    if workerType == 'task':
                        result += '%s.Task.Current: %s\n' % (
                            partitionInfoPrefix, partitionInfo.currentStatus.statusDescription)
                        result += '%s.Task.Target: %s\n' % (
                            partitionInfoPrefix, partitionInfo.targetStatus.targetDescription)

        return result

    def processWorkerStatus(self, response):
        buildAppConf = build_app_config.BuildAppConfig(self.fsUtil, self.getLatestConfig(self.configPath))
        userName = buildAppConf.userName
        serviceName = buildAppConf.serviceName
        appName = '%s_%s' % (userName, serviceName)

        hippoRoot = buildAppConf.hippoRoot
        hippoClient = HippoClient(hippoRoot)
        statusResponse = hippoClient.get_app_status(appName)

        result = self.doProcessWorkerStatus(response, statusResponse)
        return result

    def getGid(self, resourceTag):
        resourceTagVec = resourceTag.split('.')[-8:]
        return resourceTagVec[2]

    def getClusterName(self, resourceTag):
        resourceTagVec = resourceTag.split('.')[-8:]
        return resourceTagVec[-1]

    def getRole(self, resourceTag):
        resourceTagVec = resourceTag.split('.')[-8:]
        return resourceTagVec[3]

    def doProcessWorkerStatus(self, response, statusResponse):
        result = ''
        for lastAllocateResponse in statusResponse.lastAllocateResponse:
            resourceTag = str(lastAllocateResponse.resourceTag)
            if resourceTag == '__internal_appmaster_resource_tag__':
                result += self.printAdminResource(lastAllocateResponse)

        result += 'TotalWorker(expected/actual): %d/%d \n' % \
            (self.getGlobalWorkerCount(response), len(statusResponse.lastAllocateResponse) - 1)

        for generationInfo in response.generationInfos:
            prefix = '%s.%s.%d.' % (generationInfo.buildId.appName,
                                    generationInfo.buildId.dataTable,
                                    generationInfo.buildId.generationId)
            generationIdStr = str(generationInfo.buildId.generationId)
            builderCount = 0
            processorCount = 0
            mergerCount = 0
            processorResult = ''
            builderResult = ''
            mergerResult = ''
            for lastAllocateResponse in statusResponse.lastAllocateResponse:
                resourceTag = str(lastAllocateResponse.resourceTag)
                if resourceTag == '__internal_appmaster_resource_tag__':
                    continue
                if generationIdStr == self.getGid(resourceTag):
                    if 'builder' == self.getRole(resourceTag):
                        builderCount += 1
                        builderResult += self.printResourceInfo('builder', lastAllocateResponse,
                                                                resourceTag, generationInfo, resourceTag)
                    elif 'processor' == self.getRole(resourceTag):
                        processorCount += 1
                        processorResult += self.printResourceInfo('processor', lastAllocateResponse,
                                                                  resourceTag, generationInfo, resourceTag)
                    elif 'merger' == self.getRole(resourceTag):
                        mergerCount += 1
                        mergerResult += self.printResourceInfo('merger', lastAllocateResponse,
                                                               resourceTag, generationInfo, resourceTag)

            expectedBuilderCount = 0
            expectedMergerCount = 0
            expectedProcessorCount = 0
            for processorInfo in generationInfo.processorInfos:
                expectedProcessorCount += self.getProcessorExpectWorkerCount(processorInfo)
            for clusterInfo in generationInfo.buildInfo.clusterInfos:
                if len(clusterInfo.builderInfo.partitionInfos) > 0:
                    expectedBuilderCount += self.getClusterExpectWorkerCount(clusterInfo)
                else:
                    expectedMergerCount += self.getClusterExpectWorkerCount(clusterInfo)

            result += '%sTotalWorker(expected/actual): %d/%d \n' % \
                (prefix, expectedBuilderCount + expectedMergerCount + expectedProcessorCount,
                 processorCount + builderCount + mergerCount)
            result += '%sProcessor.Worker(expected/actual): %d/%d \n' % \
                (prefix, expectedProcessorCount, processorCount)
            result += '%sBuilder.Worker(expected/actual): %d/%d \n' % \
                (prefix, expectedBuilderCount, builderCount)
            result += '%sMerger.Worker(expected/actual): %d/%d \n' % \
                (prefix, expectedMergerCount, mergerCount)
            result += builderResult
            result += processorResult
            result += mergerResult
        return result

    def printAdminResource(self, lastAllocateResponse):
        result = ''
        for assignedSlot in lastAllocateResponse.assignedSlots:
            for processStatus in assignedSlot.processStatus:
                if processStatus.processName == 'bs_admin_worker':
                    startTime = self.timestampToDatetime(processStatus.startTime / 1000000)
                    processStatusStr = self.getProcessStatus(processStatus.status)
                    result += '%s.[%s %s]: %s \n' % \
                        ('admin', processStatusStr, str(startTime), assignedSlot.id.slaveAddress)
            result += 'admin.resource: ('
            resourceVec = []
            for resource in assignedSlot.slotResource.resources:
                resourceVec.append('%s: %d' % (resource.name, resource.amount))
            result += ', '.join(resourceVec) + ') \n'
        return result

    def printResourceInfo(self, workerInfo, lastAllocateResponse,
                          prefix, generationInfo=None, resourceTag=None):
        result = ''
        for assignedSlot in lastAllocateResponse.assignedSlots:
            for processStatus in assignedSlot.processStatus:
                if processStatus.processName == 'build_service_worker':
                    startTime = self.timestampToDatetime(processStatus.startTime / 1000000)
                    processStatusStr = self.getProcessStatus(processStatus.status)
                    result += '%s.[%s %s]: %s \n' % \
                        (prefix, processStatusStr, str(startTime), assignedSlot.id.slaveAddress)
            result += '%s.resource(expect/actual): (' % prefix
            expectResourceVec = []
            if workerInfo == 'processor':
                for processorInfo in generationInfo.processorInfos:
                    for resource in processorInfo.resources:
                        expectResourceVec.append('%s: %d' % (resource.name, resource.count))
                        result += ', '.join(expectResourceVec) + ') / ('
            elif workerInfo == 'builder':
                isExist = False
                for clusterInfo in generationInfo.buildInfo.clusterInfos:
                    if clusterInfo.clusterName == self.getClusterName(resourceTag):
                        isExist = True
                        break
                if isExist:
                    for resource in clusterInfo.builderInfo.resources:
                        expectResourceVec.append('%s: %d' % (resource.name, resource.count))
                result += ', '.join(expectResourceVec) + ') / ('
            elif workerInfo == 'merger':
                isExist = False
                for clusterInfo in generationInfo.buildInfo.clusterInfos:
                    if clusterInfo.clusterName == self.getClusterName(resourceTag):
                        isExist = True
                        break
                if isExist:
                    for resource in clusterInfo.mergerInfo.resources:
                        expectResourceVec.append('%s: %d' % (resource.name, resource.count))
                result += ', '.join(expectResourceVec) + ') / ('

            resourceVec = []
            for resource in assignedSlot.slotResource.resources:
                resourceVec.append('%s: %d' % (resource.name, resource.amount))
            result += ', '.join(resourceVec) + ') \n'
        return result

    def getProcessorExpectWorkerCount(self, processorInfo):
        return len(processorInfo.partitionInfos)

    def getClusterExpectWorkerCount(self, clusterInfo):
        return len(clusterInfo.builderInfo.partitionInfos) + \
            len(clusterInfo.mergerInfo.partitionInfos) + len(clusterInfo.taskInfo.partitionInfos)

    def getPendingMergeTasks(self, clusterInfo):
        return ';'.join(clusterInfo.pendingMergeTasks)

    def getConfigPath(self, partitionInfo, isTask=False):
        if isTask:
            return (partitionInfo.currentStatus.reachedTarget.configPath,
                    partitionInfo.targetStatus.configPath)
        else:
            return (partitionInfo.currentStatus.targetStatus.configPath,
                    partitionInfo.targetStatus.configPath)

    def getProcessStatus(self, status):
        if status == ProcessStatus.PS_RUNNING:
            return 'PS_RUNNING'
        if status == ProcessStatus.PS_UNKNOWN:
            return 'PS_UNKNOWN'
        if status == ProcessStatus.PS_RESTARTING:
            return 'PS_RESTARTING'
        if status == ProcessStatus.PS_STOPPING:
            return 'PS_STOPPING'
        if status == ProcessStatus.PS_STOPPED:
            return 'PS_STOPPED'
        if status == ProcessStatus.PS_FAILED:
            return 'PS_FAILED'
        if status == ProcessStatus.PS_TERMINATED:
            return 'PS_TERMINATED'

    def needPrintGeneration(self, gid):
        if len(self.generationIds) == 0:
            return True
        if gid in self.generationIds:
            return True
        return False

    def timestampToDatetime(self, timestamp, format='%Y-%m-%d %H:%M:%S'):
        return datetime.datetime.fromtimestamp(int(timestamp)).strftime(format)

    def getRangeStr(self, pRange):
        f = pRange.__getattribute__('from')
        t = pRange.__getattribute__('to')
        return '%d-%d' % (f, t)
