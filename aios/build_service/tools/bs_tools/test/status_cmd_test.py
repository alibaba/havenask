import unittest
from status_cmd import *
from build_service.proto.Admin_pb2 import *
from build_service.proto.BasicDefs_pb2 import *
from build_service.proto.Heartbeat_pb2 import *
from hippo.proto.Client_pb2 import *

class StatusCmdTest(unittest.TestCase):
    def testPrintSummary(self):
        # prepare response
        response = ServiceInfoResponse()
        response.adminAddress = '10.125.224.29:1234'
        response.adminConfig = '/path/to/admin/config'

        generationInfo = response.generationInfos.add()
        generationInfo.buildId.appName = 'test_app'
        generationInfo.buildId.generationId = 1
        generationInfo.buildId.dataTable = 'table'
        generationInfo.buildStep = 'full'
        generationInfo.configPath = '/path/to/generation/config'

        generationInfo.processorInfo.partitionCount = 1
        generationInfo.processorInfo.dataDescription = '{ "src" : "file" }'

        clusterInfo = generationInfo.buildInfo.clusterInfos.add()
        clusterInfo.clusterName = 'cluster1'
        clusterInfo.partitionCount = 2
        clusterInfo.builderInfo.partitionInfos.add()
        clusterInfo.builderInfo.partitionInfos.add()
        clusterInfo.pendingMergeTasks.append('full')
        clusterInfo.pendingMergeTasks.append('big_inc')

        cmd = GetStatusCmd()
        cmd.commandType = 'summary'
        cmd.generationIds = []

        expectPrint = '''AdminAdress: 10.125.224.29:1234
AdminConfig: /path/to/admin/config
Worker(expected): 2
test_app.table.1.BuildStep: full
test_app.table.1.ConfigPath: /path/to/generation/config
test_app.table.1.Processor.Worker(expected): 0
test_app.table.1.Processor.DataSource: src=file;
test_app.table.1.Processor.ProcessorStep: 
test_app.table.1.cluster1.ClusterStep: 
test_app.table.1.cluster1.LastVersionTimestamp: 0
test_app.table.1.cluster1.PendingMergeTasks: full;big_inc
test_app.table.1.cluster1.Builder.Worker(expected): 2
'''
        print cmd.processResponse(response, 'summary')
        self.assertEqual(expectPrint, cmd.processResponse(response, 'summary'))

    def testProcessResponseWithOnlyJobInfo(self):
        response = ServiceInfoResponse()
        response.adminAddress = '10.125.224.29:1234'
        response.adminConfig = '/path/to/admin/config'

        generationInfo = response.generationInfos.add()
        generationInfo.buildId.appName = 'test_app'
        generationInfo.buildId.generationId = 1
        generationInfo.buildId.dataTable = 'table'
        generationInfo.buildStep = 'full'
        generationInfo.configPath = '/path/to/generation/config'

        generationInfo.jobInfo.clusterName = 'cluster1'
        generationInfo.jobInfo.partitionCount = 3
        generationInfo.jobInfo.buildParallelNum = 2
        generationInfo.jobInfo.dataDescriptionId = 2
        generationInfo.jobInfo.dataDescription = '{ "src" : "file" }'
        generationInfo.jobInfo.mergeConfigName = 'full'

        cmd = GetStatusCmd()
        cmd.commandType = ''
        cmd.generationIds = []
        result = cmd.processResponse(response, '')
        print result
        expectPrint = '''AdminAdress: 10.125.224.29:1234
AdminConfig: /path/to/admin/config
Worker(expected): 0
test_app.table.1.BuildStep: full
test_app.table.1.ConfigPath: /path/to/generation/config
test_app.table.1.cluster1.Job.PartitionCount: 3
test_app.table.1.cluster1.Job.buildParallelNum: 2
test_app.table.1.cluster1.Job.dataDescriptionId: 2
test_app.table.1.cluster1.Job.mergeConfigName: full
test_app.table.1.cluster1.Job.DataSource: src=file;
'''
        self.assertEqual(expectPrint, result)
        
    def testPrintError(self):
        response = ServiceInfoResponse()
        response.adminAddress = '10.125.224.29:1234'
        response.adminConfig = '/path/to/admin/config'

        generationInfo = response.generationInfos.add()
        generationInfo.buildId.appName = 'test_app'
        generationInfo.buildId.generationId = 1
        generationInfo.buildId.dataTable = 'table'
        generationInfo.buildStep = 'full'
        generationInfo.configPath = '/path/to/generation/config'

        generationInfo.processorInfo.partitionCount = 1
        generationInfo.processorInfo.dataDescription = '{ "src" : "file" }'

        partitionInfo = generationInfo.processorInfo.partitionInfos.add()
        partitionInfo.currentStatus.longAddress.ip = "10.125.224.31:12345"
        errorInfo = partitionInfo.currentStatus.errorInfos.add()
        errorInfo.errorMsg = "process exception"
        errorInfo.errorTime = 1428911200

        partitionInfo = generationInfo.processorInfo.partitionInfos.add()
        partitionInfo.currentStatus.longAddress.ip = "10.125.224.31:12345"
        errorInfo = partitionInfo.currentStatus.errorInfos.add()
        errorInfo.errorMsg = "process fatal exception"
        errorInfo.errorTime = 1428911233

        clusterInfo = generationInfo.buildInfo.clusterInfos.add()
        clusterInfo.clusterName = 'cluster1'
        clusterInfo.partitionCount = 2

        builderPartitionInfo = clusterInfo.builderInfo.partitionInfos.add()
        builderPartitionInfo.pid.range.__setattr__('from', 0)
        builderPartitionInfo.pid.range.to = 32767
        
        errorInfo = builderPartitionInfo.currentStatus.errorInfos.add()
        builderPartitionInfo.currentStatus.longAddress.ip = "10.125.224.30:12345"
        errorInfo.errorCode = BUILDER_ERROR_BUILD_FILEIO
        errorInfo.errorMsg = "build file io exception"
        errorInfo.errorTime = 1428911295

        errorInfo = builderPartitionInfo.currentStatus.errorInfos.add()
        errorInfo.errorCode = BUILDER_ERROR_BUILD_FILEIO
        errorInfo.errorMsg = "build exception"
        errorInfo.errorTime = 1428911297

        clusterInfo = generationInfo.buildInfo.clusterInfos.add()
        clusterInfo.clusterName = 'cluster2'
        clusterInfo.partitionCount = 2

        builderPartitionInfo = clusterInfo.builderInfo.partitionInfos.add()
        builderPartitionInfo.pid.range.__setattr__('from', 123)
        builderPartitionInfo.pid.range.to = 456

        errorInfo = builderPartitionInfo.currentStatus.errorInfos.add()
        errorInfo.errorCode = BUILDER_ERROR_BUILD_FILEIO
        errorInfo.errorMsg = "build"
        errorInfo.errorTime = 456
        builderPartitionInfo.currentStatus.longAddress.ip = "abc"
        errorInfo.address = "abc"
        cmd = GetStatusCmd()
        cmd.commandType = 'error'
        cmd.generationIds = []
        expectPrint = '''test_app.table.1.Processor[0-65535].[10.125.224.31:12345].[2015-04-13 15:46:40].[process exception]
test_app.table.1.Processor[0-65535].[10.125.224.31:12345].[2015-04-13 15:47:13].[process fatal exception]
test_app.table.1.cluster1.Builder[0-32767].[10.125.224.30:12345].[2015-04-13 15:48:15].[build file io exception]
test_app.table.1.cluster1.Builder[0-32767].[10.125.224.30:12345].[2015-04-13 15:48:17].[build exception]
test_app.table.1.cluster2.Builder[123-456].[abc].[1970-01-01 08:07:36].[build]
'''
        print "======111======"
        print cmd.processErrorStatus(response)
        self.assertEqual(expectPrint, cmd.processErrorStatus(response))

    def addCounterItems(self, counterItems, partitionInfo):
        for ci in counterItems:
            counterItem = partitionInfo.counterItems.add()
            counterItem.name = ci[0] 
            counterItem.value = ci[1]
           
    def testPrintCounter(self):
        response = ServiceInfoResponse()
        response.adminAddress = '10.125.224.29:1234'
        response.adminConfig = '/path/to/admin/config'
 
        generationInfo = response.generationInfos.add()
        generationInfo.buildId.appName = 'test_app'
        generationInfo.buildId.generationId = 1
        generationInfo.buildId.dataTable = 'table'
        generationInfo.buildStep = 'full'
        generationInfo.configPath = '/path/to/generation/config'
 
        generationInfo.processorInfo.partitionCount = 1
        generationInfo.processorInfo.dataDescription = '{ "src" : "file" }'

        partitionInfo = generationInfo.processorInfo.partitionInfos.add()
        partitionInfo.currentStatus.longAddress.ip = "10.125.224.31:12345"
        partitionInfo.pid.range.__setattr__('from', 0)
        partitionInfo.pid.range.to = 1200
        self.addCounterItems([('bs.processor.swiftReadDelay', 20), ('bs.processor.parseLatency', 30)],
                             partitionInfo)
        
        partitionInfo = generationInfo.processorInfo.partitionInfos.add()
        partitionInfo.currentStatus.longAddress.ip = "10.125.227.31:12345"
        partitionInfo.pid.range.__setattr__('from', 1201)
        partitionInfo.pid.range.to = 2401
 
        self.addCounterItems([('bs.processor.estimateLeftTime', 20000), ('bs.processor.attrConvertError', 15)],
                             partitionInfo)
 
        partitionInfo = generationInfo.processorInfo.partitionInfos.add()
        partitionInfo.currentStatus.longAddress.ip = "10.125.227.31:12345"
        partitionInfo.pid.range.__setattr__('from', 2402)
        partitionInfo.pid.range.to = 3330 
 
        clusterInfo = generationInfo.buildInfo.clusterInfos.add()
        clusterInfo.clusterName = 'cluster1'
        clusterInfo.partitionCount = 2
        builderPartitionInfo = clusterInfo.builderInfo.partitionInfos.add()
        builderPartitionInfo.pid.range.__setattr__('from', 0)
        builderPartitionInfo.pid.range.to = 32767
 
        self.addCounterItems([('builder.counter1', 12), ('builder.counter2', 13)],
                             builderPartitionInfo)
        
        builderPartitionInfo = clusterInfo.builderInfo.partitionInfos.add()
        builderPartitionInfo.pid.range.__setattr__('from', 32768)
        builderPartitionInfo.pid.range.to = 65535
 
        self.addCounterItems([('builder.counter3', 14)], builderPartitionInfo)       
 
        clusterInfo = generationInfo.buildInfo.clusterInfos.add()
        clusterInfo.clusterName = 'cluster2'
        clusterInfo.partitionCount = 1
        mergePartitionInfo = clusterInfo.mergerInfo.partitionInfos.add()
        mergePartitionInfo.pid.range.__setattr__('from', 0)
        mergePartitionInfo.pid.range.to = 65535
 
        self.addCounterItems([('merger.counter1', 18), ('merger.counter2', 19)], mergePartitionInfo) 
        
        cmd = GetStatusCmd()
        cmd.commandType = 'counter'
        cmd.generationIds = []
        expectPrint = '''test_app.table.1.Processor[0-1200]
	bs.processor.swiftReadDelay : 20
	bs.processor.parseLatency : 30
test_app.table.1.Processor[1201-2401]
	bs.processor.estimateLeftTime : 20000
	bs.processor.attrConvertError : 15
test_app.table.1.cluster1.Builder[0-32767]
	builder.counter1 : 12
	builder.counter2 : 13
test_app.table.1.cluster1.Builder[32768-65535]
	builder.counter3 : 14
test_app.table.1.cluster2.Merger[0-65535]
	merger.counter1 : 18
	merger.counter2 : 19
'''
        print "======000======"
        print cmd.processCounterStatus(response)
        self.assertEqual(expectPrint, cmd.processCounterStatus(response))

        response = ServiceInfoResponse()
        response.adminAddress = '10.125.224.29:1234'
        response.adminConfig = '/path/to/admin/config'
        generationInfo = response.generationInfos.add()
        generationInfo.buildId.appName = 'test_app'
        generationInfo.buildId.generationId = 1
        generationInfo.buildId.dataTable = 'table'
        generationInfo.buildStep = 'full'
        generationInfo.configPath = '/path/to/generation/config'

        roleCounterInfo = generationInfo.roleCounterInfo.add()
        roleCounterInfo.roleName = "test.role1"
        def addCounter(ci, roleCounterInfo):
            counterItem = roleCounterInfo.counterItem.add()
            counterItem.name = ci[0]
            counterItem.value = ci[1]
        map(lambda x: addCounter(x, roleCounterInfo), (("counte1", 10), ("counter2", 20)));
        roleCounterInfo = generationInfo.roleCounterInfo.add() 
        roleCounterInfo.roleName = "test.role2"
        map(lambda x: addCounter(x, roleCounterInfo), (("counte3", 10), ("counter4", 20)));        
 
        partitionInfo = generationInfo.jobInfo.partitionInfos.add()
        partitionInfo.currentStatus.longAddress.ip = "10.125.214.31:12345"
        partitionInfo.pid.range.__setattr__('from', 0)
        partitionInfo.pid.range.to = 1200
        self.addCounterItems([('bs.job.counter1', 20), ('bs.job.counter2', 30)], partitionInfo) 
 
        partitionInfo = generationInfo.jobInfo.partitionInfos.add()
        partitionInfo.currentStatus.longAddress.ip = "10.125.204.31:12345"
        partitionInfo.pid.range.__setattr__('from', 1201)
        partitionInfo.pid.range.to = 2200
        self.addCounterItems([('bs.job.counter3', 120), ('bs.job.counter4', 130)], partitionInfo)        
 
        partitionInfo = generationInfo.jobInfo.partitionInfos.add()
        partitionInfo.currentStatus.longAddress.ip = "10.125.204.31:12345"
        partitionInfo.pid.range.__setattr__('from', 2201)
        partitionInfo.pid.range.to = 3200
 
        print "======000======"
        print cmd.processCounterStatus(response)
        expectPrint ='''test_app.table.1.Job[0-1200]
	bs.job.counter1 : 20
	bs.job.counter2 : 30
test_app.table.1.Job[1201-2200]
	bs.job.counter3 : 120
	bs.job.counter4 : 130
'''
        self.assertEqual(expectPrint, cmd.processCounterStatus(response)) 

        print "======show all counters======"
        cmd.showAllCounters = True
        actualPrint = cmd.processCounterStatus(response)
        expectPrint = '''test.role1:
	counte1 : 10
	counter2 : 20
test.role2:
	counte3 : 10
	counter4 : 20
'''
        self.assertEqual(expectPrint, actualPrint)
        
    def testJobJobStatus(self):
        jobInfo = JobInfo()
        jobInfo.clusterName = 'cluster1'
        jobInfo.partitionCount = 3
        jobInfo.buildParallelNum = 2
        jobInfo.dataDescriptionId = 2
        jobInfo.dataDescription = '{ "src" : "file" }'
        jobInfo.mergeConfigName = 'full'

        cmd = GetStatusCmd()
        cmd.commandType = 'detail'
        cmd.generationIds = []
        result = cmd.printJobStatus('test_app.table.1', jobInfo, 'detail')
        expectPrint = '''test_app.table.1.cluster1.Job.PartitionCount: 3
test_app.table.1.cluster1.Job.buildParallelNum: 2
test_app.table.1.cluster1.Job.dataDescriptionId: 2
test_app.table.1.cluster1.Job.mergeConfigName: full
test_app.table.1.cluster1.Job.DataSource: src=file;
'''
        print result
        self.assertEqual(expectPrint, result)
        
    def testPrintDetail(self):
        # prepare response
        response = ServiceInfoResponse()
        response.adminAddress = '10.125.224.29:1234'
        response.adminConfig = '/path/to/admin/config'

        generationInfo = response.generationInfos.add()
        generationInfo.buildId.appName = 'test_app'
        generationInfo.buildId.generationId = 1
        generationInfo.buildId.dataTable = 'table'
        generationInfo.buildStep = 'full'
        generationInfo.configPath = '/path/to/generation/config'

        generationInfo.processorInfo.partitionCount = 2
        generationInfo.processorInfo.dataDescription = '{ "src" : "file" }'
        generationInfo.processorInfo.processorStep = 'full'
        generationInfo.processorInfo.parallelNum = 1
        processorPartitionInfo1 = generationInfo.processorInfo.partitionInfos.add()
        processorPartitionInfo1.pid.range.__setattr__('from', 0)
        processorPartitionInfo1.pid.range.to = 32767
        processorPartitionInfo1.currentStatus.longAddress.ip = '/path1'
        processorPartitionInfo1.currentStatus.targetStatus.configPath = '/path2'
        
        processorPartitionInfo1.targetStatus.configPath = '/path1'
        processorPartitionInfo1.targetStatus.dataDescription = 'dataDescription'

        processorPartitionInfo2 = generationInfo.processorInfo.partitionInfos.add()
        processorPartitionInfo2.pid.range.to = 65535
        processorPartitionInfo2.pid.range.__setattr__('from', 32768)
        
        processorPartitionInfo2.currentStatus.longAddress.ip = '/path1'
        processorPartitionInfo2.currentStatus.targetStatus.configPath = '/path2'
        processorPartitionInfo2.currentStatus.targetStatus.configPath = 'file'
        
        processorPartitionInfo2.targetStatus.configPath = '/path1'
        processorPartitionInfo2.targetStatus.dataDescription = 'dataDescription'

        clusterInfo = generationInfo.buildInfo.clusterInfos.add()
        clusterInfo.clusterName = 'cluster1'
        clusterInfo.partitionCount = 2
        clusterInfo.clusterStep = 'MS_DO_MERGE'
        clusterInfo.mergerInfo.parallelNum = 2

        clusterInfo.pendingMergeTasks.append('optimize')
        clusterInfo.pendingMergeTasks.append('optimize')
        
        partitionInfo = clusterInfo.mergerInfo.partitionInfos.add()
        partitionInfo.pid.range.__setattr__('from', 0)
        partitionInfo.pid.range.to = 32767
        partitionInfo.targetStatus.configPath = 'path1'
        partitionInfo.targetStatus.mergeTask.mergeConfigName = 'optimize'
        partitionInfo.currentStatus.longAddress.ip = '10.125.224.29:12345'
        partitionInfo.currentStatus.targetStatus.configPath = 'path1'
        partitionInfo.currentStatus.targetStatus.mergeTask.mergeConfigName = 'optimize'

        clusterInfo = generationInfo.buildInfo.clusterInfos.add()
        clusterInfo.clusterName = 'cluster2'
        clusterInfo.partitionCount = 3
        clusterInfo.builderInfo.parallelNum = 2
        clusterInfo.lastVersionTimestamp = 100
        clusterInfo.clusterStep = 'full_building'

        clusterInfo.pendingMergeTasks.append('full')
        clusterInfo.pendingMergeTasks.append('full')
        
        partitionInfo = clusterInfo.builderInfo.partitionInfos.add()
        
        partitionInfo.pid.range.__setattr__('from', 32768)
        partitionInfo.pid.range.to = 65535
        partitionInfo.targetStatus.configPath = 'path2'
        partitionInfo.targetStatus.startTimestamp = 10000
        partitionInfo.currentStatus.longAddress.ip = '10.125.224.29:12345'
        partitionInfo.currentStatus.status = WS_STARTED
        partitionInfo.currentStatus.targetStatus.configPath = 'path2'
        partitionInfo.currentStatus.targetStatus.startTimestamp = 1000000

        cmd = GetStatusCmd()
        cmd.commandType = 'detail'
        cmd.generationIds = []

        expectPrint = '''AdminAdress: 10.125.224.29:1234
AdminConfig: /path/to/admin/config
Worker(expected): 4
test_app.table.1.BuildStep: full
test_app.table.1.ConfigPath: /path/to/generation/config
test_app.table.1.Processor.Worker(expected): 2
test_app.table.1.Processor.DataSource: src=file;
test_app.table.1.Processor.ProcessorStep: full
test_app.table.1.Processor.Range[0-32767].Address: /path1
test_app.table.1.Processor.Range[0-32767].Config.Current: /path2
test_app.table.1.Processor.Range[0-32767].Config.Target: /path1
test_app.table.1.Processor.Range[0-32767].StartLocator.Target: (0, 0)
test_app.table.1.Processor.Range[0-32767].StartLocator.Current: (0, 0)
test_app.table.1.Processor.Range[0-32767].StopTimestamp.Target: 0
test_app.table.1.Processor.Range[0-32767].StopTimestamp.Current: 0
test_app.table.1.Processor.Range[0-32767].Status: WS_UNKNOWN
test_app.table.1.Processor.Range[0-32767].StopLocator: (0, 0)
test_app.table.1.Processor.Range[32768-65535].Address: /path1
test_app.table.1.Processor.Range[32768-65535].Config.Current: file
test_app.table.1.Processor.Range[32768-65535].Config.Target: /path1
test_app.table.1.Processor.Range[32768-65535].StartLocator.Target: (0, 0)
test_app.table.1.Processor.Range[32768-65535].StartLocator.Current: (0, 0)
test_app.table.1.Processor.Range[32768-65535].StopTimestamp.Target: 0
test_app.table.1.Processor.Range[32768-65535].StopTimestamp.Current: 0
test_app.table.1.Processor.Range[32768-65535].Status: WS_UNKNOWN
test_app.table.1.Processor.Range[32768-65535].StopLocator: (0, 0)
test_app.table.1.cluster1.ClusterStep: MS_DO_MERGE
test_app.table.1.cluster1.LastVersionTimestamp: 0
test_app.table.1.cluster1.PendingMergeTasks: optimize;optimize
test_app.table.1.cluster1.Merger.Worker(expected): 1
test_app.table.1.cluster1.Merger.Range[0-32767].Address.10.125.224.29:12345
test_app.table.1.cluster1.Merger.Range[0-32767].Config.Current: path1
test_app.table.1.cluster1.Merger.Range[0-32767].Config.Target: path1
test_app.table.1.cluster1.Merger.Range[0-32767].MergeConfigName.Current: optimize
test_app.table.1.cluster1.Merger.Range[0-32767].MergeConfigName.Target: optimize
test_app.table.1.cluster1.Merger.Range[0-32767].Timestamp.Current: -1
test_app.table.1.cluster1.Merger.Range[0-32767].Timestamp.Target: -1
test_app.table.1.cluster2.ClusterStep: full_building
test_app.table.1.cluster2.LastVersionTimestamp: 100
test_app.table.1.cluster2.PendingMergeTasks: full;full
test_app.table.1.cluster2.Builder.Worker(expected): 1
test_app.table.1.cluster2.Builder.Range[32768-65535].Address.10.125.224.29:12345
test_app.table.1.cluster2.Builder.Range[32768-65535].Config.Current: path2
test_app.table.1.cluster2.Builder.Range[32768-65535].Config.Target: path2
test_app.table.1.cluster2.Builder.Range[32768-65535].StartTimestamp.Current: 1000000
test_app.table.1.cluster2.Builder.Range[32768-65535].StartTimestamp.Target: 10000
test_app.table.1.cluster2.Builder.Range[32768-65535].StopTimestamp.Current: 0
test_app.table.1.cluster2.Builder.Range[32768-65535].StopTimestamp.Target: 0
test_app.table.1.cluster2.Builder.Range[32768-65535].Status: WS_STARTED
'''
        print cmd.processResponse(response, 'detail')
        self.assertEqual(expectPrint, cmd.processResponse(response, 'detail'))

    def testSourceStatus(self):
        response = ServiceInfoResponse()
        response.adminAddress = '10.125.224.29:1234'
        response.adminConfig = '/path/to/admin/config'

        generationInfo = response.generationInfos.add()
        generationInfo.buildId.appName = 'test_app'
        generationInfo.buildId.generationId = 1
        generationInfo.buildId.dataTable = 'table'
        dataDescriptions = [
            {
                'src' : 'file1',
                'type' : 'file',
                'dump_version' : '123123',
            },
            {
                'src' : 'swift1',
                'type' : 'swift',
                'k' : 'v',
            }
        ]
        generationInfo.dataDescriptions = json.write(dataDescriptions)

        generationInfo = response.generationInfos.add()
        generationInfo.buildId.appName = 'test_app'
        generationInfo.buildId.generationId = 100
        generationInfo.buildId.dataTable = 'table2'
        dataDescriptions = [
            {
                'src' : 'hbase1',
                'type' : 'hbase',
                'k1' : 'v1',
            }
        ]
        generationInfo.dataDescriptions = json.write(dataDescriptions)

        expectPrint = '''test_app.table.1.file1.src: file1
test_app.table.1.file1.dump_version: 123123
test_app.table.1.file1.type: file
test_app.table.1.swift1.src: swift1
test_app.table.1.swift1.k: v
test_app.table.1.swift1.type: swift
test_app.table2.100.hbase1.src: hbase1
test_app.table2.100.hbase1.k1: v1
test_app.table2.100.hbase1.type: hbase
'''
        cmd = GetStatusCmd()
        print cmd.processSourceStatus(response)
        self.assertEqual(expectPrint, cmd.processSourceStatus(response))

    def testProcessWorkerStatus(self):
        response = ServiceInfoResponse()
        response.adminAddress = '10.125.224.29:1234'

        generationInfo1 = response.generationInfos.add()
        generationInfo1.buildId.appName = 'test_app'
        generationInfo1.buildId.generationId = 1
        generationInfo1.buildId.dataTable = 'table'
        generationInfo1.buildStep = 'full'

        generationInfo1.processorInfo.partitionCount = 1
        generationInfo1.processorInfo.dataDescription = '{ "src" : "file" }'
        generationInfo1.processorInfo.parallelNum = 1
        generationInfo1.processorInfo.partitionInfos.add()
        bsResource1 = generationInfo1.processorInfo.resources.add()
        bsResource1.name = 'cpu'
        bsResource1.count = 100

        bsResource2 = generationInfo1.processorInfo.resources.add()
        bsResource2.name = 'mem'
        bsResource2.count = 200

        clusterInfo1 = generationInfo1.buildInfo.clusterInfos.add()
        clusterInfo1.clusterName = 'simple'
        clusterInfo1.partitionCount = 1
        clusterInfo1.clusterStep = 'full_building'
        clusterInfo1.builderInfo.partitionInfos.add()
        bsResource3 = clusterInfo1.builderInfo.resources.add()
        bsResource3.name = 'cpu'
        bsResource3.count = 300

        bsResource4 = clusterInfo1.builderInfo.resources.add()
        bsResource4.name = 'mem'
        bsResource4.count = 400

        clusterInfo2 = generationInfo1.buildInfo.clusterInfos.add()
        clusterInfo2.clusterName = 'simple2'
        clusterInfo2.partitionCount = 1
        clusterInfo2.clusterStep = 'merging'
        clusterInfo2.mergerInfo.partitionInfos.add()
        bsResource5 = clusterInfo2.mergerInfo.resources.add()
        bsResource5.name = 'cpu'
        bsResource5.count = 500

        bsResource6 = clusterInfo2.mergerInfo.resources.add()
        bsResource6.name = 'mem'
        bsResource6.count = 600

        statusResponse = AppStatusResponse()
        
        resourceResponse1 = statusResponse.lastAllocateResponse.add()
        resourceResponse1.resourceTag = 'app.simple.1.builder.full.0.65535.simple'
        assignedSlot1 = resourceResponse1.assignedSlots.add()
        resource1 = assignedSlot1.slotResource.resources.add()
        resource1.name = 'cpu'
        resource1.amount = 100
        resource2 = assignedSlot1.slotResource.resources.add()
        resource2.name = 'mem'
        resource2.amount = 200
        
        processStatus1 = assignedSlot1.processStatus.add()
        processStatus1.startTime = 1428911297000000
        processStatus1.status = ProcessStatus.PS_STOPPING
        processStatus1.processName = 'build_service_worker'
        
        assignedSlot1.id.slaveAddress = '10.125.224.29:1234'

        resourceResponse2 = statusResponse.lastAllocateResponse.add()
        resourceResponse2.resourceTag = 'app.simple.1.processor.full.0.65535'
        assignedSlot2 = resourceResponse2.assignedSlots.add()
        resource3 = assignedSlot2.slotResource.resources.add()
        resource3.name = 'cpu'
        resource3.amount = 300
        resource4 = assignedSlot2.slotResource.resources.add()
        resource4.name = 'mem'
        resource4.amount = 400
        
        processStatus2 = assignedSlot2.processStatus.add()
        processStatus2.startTime = 1428911297000000
        processStatus2.status = ProcessStatus.PS_STOPPING
        processStatus2.processName = 'build_service_worker'

        assignedSlot2.id.slaveAddress = '10.125.224.29:1234'

        resourceResponse3 = statusResponse.lastAllocateResponse.add()
        resourceResponse3.resourceTag =  '__internal_appmaster_resource_tag__'
        assignedSlot3 = resourceResponse3.assignedSlots.add()
        resource5 = assignedSlot3.slotResource.resources.add()
        resource5.name = 'cpu'
        resource5.amount = 500
        resource6 = assignedSlot3.slotResource.resources.add()
        resource6.name = 'mem'
        resource6.amount = 600
        
        processStatus3 = assignedSlot3.processStatus.add()
        processStatus3.startTime = 1428911297000000
        processStatus3.status = ProcessStatus.PS_STOPPING
        processStatus3.processName = 'bs_admin_worker'
        
        assignedSlot3.id.slaveAddress = '10.125.224.29:1234'

        resourceResponse4 = statusResponse.lastAllocateResponse.add()
        resourceResponse4.resourceTag = 'app.simple.1.merger.inc_merge.0.65535.simple2'
        assignedSlot4 = resourceResponse4.assignedSlots.add()
        resource7 = assignedSlot4.slotResource.resources.add()
        resource7.name = 'cpu'
        resource7.amount = 700
        resource8 = assignedSlot4.slotResource.resources.add()
        resource8.name = 'mem'
        resource8.amount = 800
        
        processStatus4 = assignedSlot4.processStatus.add()
        processStatus4.startTime = 1428911297000000
        processStatus4.status = ProcessStatus.PS_STOPPING
        processStatus4.processName = 'build_service_worker'

        assignedSlot4.id.slaveAddress = '10.125.224.29:1234'

        expectPrint = '''admin.[PS_STOPPING 2015-04-13 15:48:17]: 10.125.224.29:1234 
admin.resource: (cpu: 500, mem: 600) 
TotalWorker(expected/actual): 3/3 
test_app.table.1.TotalWorker(expected/actual): 3/3 
test_app.table.1.Processor.Worker(expected/actual): 1/1 
test_app.table.1.Builder.Worker(expected/actual): 1/1 
test_app.table.1.Merger.Worker(expected/actual): 1/1 
app.simple.1.builder.full.0.65535.simple.[PS_STOPPING 2015-04-13 15:48:17]: 10.125.224.29:1234 
app.simple.1.builder.full.0.65535.simple.resource(expect/actual): (cpu: 300, mem: 400) / (cpu: 100, mem: 200) 
app.simple.1.processor.full.0.65535.[PS_STOPPING 2015-04-13 15:48:17]: 10.125.224.29:1234 
app.simple.1.processor.full.0.65535.resource(expect/actual): (cpu: 100, mem: 200) / (cpu: 300, mem: 400) 
app.simple.1.merger.inc_merge.0.65535.simple2.[PS_STOPPING 2015-04-13 15:48:17]: 10.125.224.29:1234 
app.simple.1.merger.inc_merge.0.65535.simple2.resource(expect/actual): (cpu: 500, mem: 600) / (cpu: 700, mem: 800) 
'''
        cmd = GetStatusCmd()
        self.assertEqual(expectPrint, cmd.doProcessWorkerStatus(response, statusResponse))
