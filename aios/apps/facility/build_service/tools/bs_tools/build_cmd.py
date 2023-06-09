# -*- coding: utf-8 -*-

import include.json as json

from build_service.proto.Admin_pb2 import StartBuildRequest
from build_service.proto.Admin_pb2 import StopBuildRequest
from build_service.proto.Admin_pb2 import UpdateConfigRequest
from build_service.proto.Admin_pb2 import StepBuildRequest
from build_service.proto.Admin_pb2 import CreateVersionRequest
from build_service.proto.Admin_pb2 import SwitchBuildRequest
from build_service.proto.Admin_pb2 import AdminService_Stub

from admin_cmd_base import AdminCmdBase
import admin_locator
import arpc.python.rpc_impl_async as async_rpc_impl
from generation_manager import GenerationManager
from common_define import *
import build_app_config
import build_rule_config
import range_util
import os, re

class StartBuildCmd(AdminCmdBase):
    '''
    bs startbuild -c config_path

options:
    -c config_path               : required, config path for build Build
    -g generation_id             : optional, start build from an existing generation
    -a app_name                  : optional, application name
    -d data_table                : required, data table to build
    -m build_mode                : optional, full|incremental, default is full
    --debug_mode                 : optional, run|step, default is run
    --data_description_file      : optional, data description(in file) to build
    --data_description_kv        : optional, data description(kv pair) to build, "&&", ";", "=" is not allowed in kv
    --gm                         : optional, generation meta for data_table
    --timeout                    : optional, timeout to call admin
    --job                        : optional, enable job mode, make processor and builder in sinlge process
examples:
    bs startbuild -c /path/to/config -d simple
    bs startbuild -c /path/to/config -d simple --gm cluster1=cluster1_generation_meta;cluster2=cluster2_generation_meta
    bs startbuild -c /path/to/config -d simple --data_description_file data.json
    bs startbuild -c /path/to/config -d simple --data_description_kv src=file&&type=file&&data=a.dat&&document_format=ha3;   src=swift&&type=swift&&swift_root=zfs&&topic_name=name
    '''
    ADMIN_START_BUILD_METHOD = 'startBuild'

    def __init__(self):
        super(StartBuildCmd, self).__init__()

    def addOptions(self):
        super(StartBuildCmd, self).addOptions()
        self.parser.add_option('-d', '--data_table', action='store', dest='dataTable')
        self.parser.add_option('-g', '--generation_id', action='store',
                               dest='generationId', type='int')
        self.parser.add_option('-m', '--build_mode', action='store', type='choice',
                               choices=['full', 'inc', 'incremental', 'customized'],
                               dest='buildMode', default='full')
        self.parser.add_option('', '--debug_mode', action='store', type='choice',
                               choices=['run', 'step'],
                               dest='debugMode', default='run')
        self.parser.add_option('', '--data_description_file', action='store',
                               dest='dataDescriptionFile')
        self.parser.add_option('', '--data_description_kv', action='store',
                               dest='dataDescriptionKv')
        self.parser.add_option('', '--gm', action='store', dest='generationMeta')
        self.parser.add_option('', '--job', action='store_true', dest='jobMode')

    def checkOptionsValidity(self, options):
        super(StartBuildCmd, self).checkOptionsValidity(options)
        if options.generationId is not None :
            GenerationManager.checkGenerationId(options.generationId)
        if options.dataTable is None:
            raise Exception('-d data_table is needed.')
        if options.dataDescriptionKv is not None and options.dataDescriptionFile is not None:
            raise Exception('only one of --data_description_file and --data_description_kv can be use once')

    def initMember(self, options):
        super(StartBuildCmd, self).initMember(options)
        self.dataTable = options.dataTable
        self.generationId = options.generationId
        if self.generationId is None:
            import time
            self.generationId = int(time.time())
        self.buildMode = options.buildMode
        if self.buildMode != 'full' and self.buildMode != 'customized':
            self.buildMode = 'incremental'
        self.debugMode = options.debugMode
        self.dataDescriptionKv = options.dataDescriptionKv
        self.dataDescriptionFile = options.dataDescriptionFile
        self.latestConfig = self.getLatestConfig(self.configPath)
        generationMetaInConfig = os.path.join(self.latestConfig, 'generation_meta')
        if self.fsUtil.exists(generationMetaInConfig):
            if options.generationMeta:
                raise Exception('Can\'t specify generation meta in both config and command!')
            self.generationMeta = generationMetaInConfig
        else:
            self.generationMeta = options.generationMeta
        self.jobMode = options.jobMode

    def __getClusterNames(self, configPath, dataTable):
        fileName = os.path.join(configPath, 'data_tables/%s_table.json' % dataTable)
        content = self.fsUtil.cat(fileName)
        jsonMap = json.read(content)
        return reduce(lambda x,y:x+y, map(lambda x: x['clusters'], jsonMap['processor_chain_config']))

    def copyGenerationMeta(self, configPath, dataTable, generationId, generationMeta):
        clusterNames = self.__getClusterNames(configPath, dataTable)
        buildAppConf = build_app_config.BuildAppConfig(self.fsUtil, configPath)
        for clusterName in clusterNames:
            dstDir = '%s/%s/generation_%s' % (buildAppConf.indexRoot, clusterName, str(generationId))
            if not self.fsUtil.exists(dstDir):
                self.fsUtil.mkdir(dstDir)
            dstFile = dstDir + '/generation_meta'
            if self.fsUtil.exists(dstFile):
                print "WARN: generation_meta has exist for cluster[%s]/generation_%s" % (
                    clusterName, str(generationId))
                continue
            self.fsUtil.copy(generationMeta, dstFile)

    def createRequest(self):
        request = StartBuildRequest()
        request.configPath = self.latestConfig
        request.buildId.appName = self.appName
        request.buildId.dataTable = self.dataTable
        request.buildId.generationId = self.generationId
        request.buildMode = self.buildMode
        request.debugMode = self.debugMode
        request.dataDescriptionKvs = self._transformDataDesc(
            request.configPath, self.dataTable,
            self.dataDescriptionKv, self.dataDescriptionFile)
        if self.jobMode:
            request.jobMode = True
        return request

    # return empty json map when dataDescriptionKv=None and dataDescriptionFile=None
    # return valid data description in json string else
    # raise exception when they are invalid
    def _transformDataDesc(self, configPath, dataTable,
                           dataDescriptionKv=None, dataDescriptionFile=None):
        cmdDataDescriptions = []
        if dataDescriptionKv is not None:
            dataDescriptionsStr = filter(None, dataDescriptionKv.split(';'))
            for dataDescriptionStr in dataDescriptionsStr:
                dataDescription = {}
                kvs = filter(None, dataDescriptionStr.split('&&'))
                for kvStr in kvs:
                    kvpair = kvStr.split('=')
                    if len(kvpair) < 2:
                        raise Exception('Invalid key value string [%s], expect format [key=value]' % kvStr)
                    dataDescription[kvpair[0]] = '='.join(kvpair[1:])
                cmdDataDescriptions.append(dataDescription)
        elif dataDescriptionFile is not None:
            cmdDataDescriptions = json.read(self.fsUtil.cat(dataDescriptionFile))

        # read dataDescription from config
        configDataDescriptions = self._getConfigDataDescriptions(configPath, dataTable)

        if len(cmdDataDescriptions) == 0:
            return json.write(configDataDescriptions)
        elif len(configDataDescriptions) == 0:
            return json.write(cmdDataDescriptions)
        else:
            dataDescriptions = self._mergeDataDescriptions(cmdDataDescriptions, configDataDescriptions)
            return json.write(dataDescriptions)

    def _getConfigDataDescriptions(self, configPath, dataTable):
        content = self.fsUtil.cat(os.path.join(
                configPath, DATA_TABLES_DIR_NAME + '/' + dataTable + DATA_TABLE_FILE_SUFFIX))
        jsonMap = json.read(content)
        if jsonMap.has_key('data_descriptions'):
            return jsonMap['data_descriptions']
        else:
            return []

    def _mergeDataDescriptions(self, descriptions1, descriptions2):
        '''
        merge des1 to des2
        '''
        update_count = min(len(descriptions1), len(descriptions2))
        for i in xrange(update_count):
            descriptions2[i].update(descriptions1[i])
        for i in range(update_count, len(descriptions1)):
            descriptions2.append(descriptions1[i])
        return descriptions2

    def _checkForIncBuild(self, configPath, dataTable, generationId):
        # check partition count and range coverage, support build from empty full ?
        # check schema if not build from empty full
        buildAppConf = build_app_config.BuildAppConfig(self.fsUtil, configPath)
        indexRoot = buildAppConf.indexRoot
        clusterNames = self.__getClusterNames(configPath, dataTable)
        for clusterName in clusterNames:
            self._checkOneCluster(configPath, indexRoot, clusterName, generationId)

    def _checkOneCluster(self, configPath, indexRoot, clusterName, generationId):
        buildRuleConfig = build_rule_config.BuildRuleConfig(self.fsUtil, clusterName, configPath)
        indexPath = '/'.join([indexRoot, clusterName, 'generation_%d' % generationId])
        if not self.fsUtil.exists(indexPath):
            # full build not started, just return
            return
        allParts = self.fsUtil.listDir(indexPath)
        pattern = 'partition_\d+_\d+'
        allParts = filter(lambda x: re.match(pattern, x), allParts)
        expectParts = self._generatePartitions(buildRuleConfig.partitionCount)
        allParts.sort()
        expectParts.sort()
        if allParts != expectParts:
            raise Exception('full partition[%s] does not equal inc partition[%s]' % (
                    ','.join(allParts), ','.join(expectParts)))

        for part in allParts:
            schemaFile = '/'.join([indexPath, part, 'schema.json'])
            if not self.fsUtil.exists(schemaFile):
                raise Exception('schemaFile[%s] does not exist' % schemaFile)

    def _generatePartitions(self, partitionCount):
        allRanges = range_util.RangeUtil.splitRange(partitionCount)
        return map(lambda x: 'partition_%d_%d' % (x[0], x[1]), allRanges)

    def run(self):
        if self.buildMode == 'incremental':
            self._checkForIncBuild(self.latestConfig, self.dataTable, self.generationId)
        if self.buildMode == 'full' and self.generationMeta is not None:
            self.copyGenerationMeta(self.latestConfig, self.dataTable, self.generationId, self.generationMeta)
        response = self.call(self.ADMIN_START_BUILD_METHOD)
        from include.protobuf_json import pb2json
        print json.write(pb2json(response), format=True)

class StopBuildCmd(AdminCmdBase):
    '''
    bs stopbuild -c config_path

options:
    -c config_path               : required, config path for build Build
    -a app_name                  : optional, app name for build
    -d data_table                : optional, data table to build
    -g generation_id             : optional, generation to stop

examples:
    bs stopbuild -c /path/to/config -a app_name   --> stop all generations from the same app
    bs stopbuild -c /path/to/config -a app_name -d data_table --> stop all generations from the same data_table
    bs stopbuild -c /path/to/config -a app_name -d data_table -g generationId --> stop a specified generation
    '''
    ADMIN_STOP_BUILD_METHOD = 'stopBuild'

    def __init__(self):
        super(StopBuildCmd, self).__init__()

    def addOptions(self):
        super(StopBuildCmd, self).addOptions()
        self.parser.add_option('-d', '--data_table', action='store', dest='dataTable')
        self.parser.add_option('-g', '--generation_id', action='store', dest='generationId',
                               type='int')

    def checkOptionsValidity(self, options):
        super(StopBuildCmd, self).checkOptionsValidity(options)
        if options.generationId is not None and options.dataTable is None:
            raise Exception('data_table must be specified when generationId is specified')
        if options.generationId is not None:
            GenerationManager.checkGenerationId(options.generationId)

    def initMember(self, options):
        super(StopBuildCmd, self).initMember(options)
        self.generationId = options.generationId
        self.dataTable = options.dataTable

    def createRequest(self):
        request = StopBuildRequest()
        request.configPath = self.getLatestConfig(self.configPath)
        request.buildId.appName = self.appName
        if self.dataTable is not None:
            request.buildId.dataTable = self.dataTable
        if self.generationId is not None:
            request.buildId.generationId = self.generationId
        return request

    def run(self):
        self.call(self.ADMIN_STOP_BUILD_METHOD)

class UpdateConfigCmd(AdminCmdBase):
    '''
    bs upc -c config_path -d data_table -g generation_id

options:
    -c config_path               : required, config path update
    -a app_name                  : optional, app name to build
    -d data_table                : required, data table to build
    -g generation_id             : required, generation to stop

examples:
    bs updateconfig -c /path/to/config -d simple -g 12345678
    '''
    ADMIN_UPDATE_CONFIG_METHOD = 'updateConfig'

    def addOptions(self):
        super(UpdateConfigCmd, self).addOptions()
        self.parser.add_option('-d', '--data_table', action='store', dest='dataTable')
        self.parser.add_option('-g', '--generation_id', action='store',
                               dest='generationId', type='int')

    def checkOptionsValidity(self, options):
        super(UpdateConfigCmd, self).checkOptionsValidity(options)
        if options.generationId is None:
            raise Exception('-g generation_id is needed.')
        GenerationManager.checkGenerationId(options.generationId)
        if options.dataTable is None:
            raise Exception('-d data_table is needed.')

    def initMember(self, options):
        super(UpdateConfigCmd, self).initMember(options)
        self.generationId = options.generationId
        self.dataTable = options.dataTable

    def createRequest(self):
        request = UpdateConfigRequest()
        request.configPath = self.getLatestConfig(self.configPath)
        request.buildId.generationId = self.generationId
        request.buildId.dataTable = self.dataTable
        request.buildId.appName = self.appName
        return request

    def run(self):
        self.call(self.ADMIN_UPDATE_CONFIG_METHOD)

class CreateVersionCmd(AdminCmdBase):
    '''
    bs createversion -c config_path

options:
    -c config_path               : required, config path for build Build
    -a app_name                  : optional, app name to build
    -d data_table                : required, data table for create version
    -g generation_id             : required, generation for create version
    -n cluster_name              : required, cluster to create version
examples:
    bs createversion -c /path/to/config -n cluster_name
    '''

    ADMIN_CREATE_VERSION_METHOD = 'createVersion'

    def __init__(self):
        super(CreateVersionCmd, self).__init__()

    def addOptions(self):
        super(CreateVersionCmd, self).addOptions()
        self.parser.add_option('-d', '--data_table', action='store', dest='dataTable')
        self.parser.add_option('-g', '--generation_id', action='store', dest='generationId',
                               type='int')
        self.parser.add_option('-n', '--cluster_name', action='store', dest='clusterName')
        self.parser.add_option('-m', '--mergeconfig', action='store', dest='mergeConfigName', default='')

    def checkOptionsValidity(self, options):
        super(CreateVersionCmd, self).checkOptionsValidity(options)
        if options.dataTable is None:
            raise Exception('-d data_table is required.')
        if options.generationId is None:
            raise Exception('-g generation_id is required.')
        GenerationManager.checkGenerationId(options.generationId)
        if options.clusterName is None:
            raise Exception('-n clusterName is required.')

    def initMember(self, options):
        super(CreateVersionCmd, self).initMember(options)
        self.dataTable = options.dataTable
        self.generationId = options.generationId
        self.clusterName = options.clusterName
        self.mergeConfigName = options.mergeConfigName

    def createRequest(self):
        request = CreateVersionRequest()
        request.buildId.appName = self.appName
        request.buildId.generationId = self.generationId
        request.buildId.dataTable = self.dataTable
        request.mergeConfig = self.mergeConfigName
        request.clusterName = self.clusterName
        return request

    def run(self):
        self.call(self.ADMIN_CREATE_VERSION_METHOD)

class SwitchCmd(AdminCmdBase):
    '''
    bs switch -c config_path

options:
    -c config_path               : required, config path for build Build
    -a app_name                  : optional, app name to build
    -d data_table                : required, data table for switch
    -g generation_id             : required, generation for switch
examples:
    bs switch -c /path/to/config -d data_table -g gid
    '''

    ADMIN_SWITCH_METHOD = 'switchBuild'

    def __init__(self):
        super(SwitchCmd, self).__init__()

    def addOptions(self):
        super(SwitchCmd, self).addOptions()
        self.parser.add_option('-d', '--data_table', action='store', dest='dataTable')
        self.parser.add_option('-g', '--generation_id', action='store', dest='generationId',
                               type='int')

    def checkOptionsValidity(self, options):
        super(SwitchCmd, self).checkOptionsValidity(options)
        if options.dataTable is None:
            raise Exception('-d data_table is required.')
        if options.generationId is None:
            raise Exception('-g generation_id is required.')

    def initMember(self, options):
        super(SwitchCmd, self).initMember(options)
        self.dataTable = options.dataTable
        self.generationId = options.generationId

    def createRequest(self):
        request = SwitchBuildRequest()
        request.buildId.appName = self.appName
        request.buildId.generationId = self.generationId
        request.buildId.dataTable = self.dataTable
        return request

    def run(self):
        self.call(self.ADMIN_SWITCH_METHOD)

class StepBuildCmd(AdminCmdBase):
    '''
    bs stepbuild -d data_table -g generation_id

options:
    -a app_name                  : optional, app name to build
    -d data_table                : required, data table for switch
    -g generation_id             : required, generation for switch
examples:
    bs stepbuild -d data_table -g gid
    '''

    ADMIN_STEPBUILD_METHOD = 'stepBuild'

    def __init__(self):
        super(StepBuildCmd, self).__init__()

    def addOptions(self):
        super(StepBuildCmd, self).addOptions()
        self.parser.add_option('-z', '--zookeeper', action='store', dest='zkRoot')
        self.parser.add_option('-d', '--data_table', action='store', dest='dataTable')
        self.parser.add_option('-g', '--generation_id', action='store', dest='generationId',
                               type='int')

    def checkOptionsValidity(self, options):
        if options.dataTable is None:
            raise Exception('-d data_table is required.')
        if options.generationId is None:
            raise Exception('-g generation_id is required.')
        if not options.configPath and not options.zkRoot:
            raise Exception('-c config_path or -z zookeeper_root is needed.')

    def initMember(self, options):
        super(StepBuildCmd, self).initMember(options)
        self.dataTable = options.dataTable
        self.generationId = options.generationId
        self.zkRoot = options.zkRoot

    def createRequest(self):
        request = StepBuildRequest()
        request.buildId.appName = self.appName
        request.buildId.generationId = self.generationId
        request.buildId.dataTable = self.dataTable
        return request

    def run(self):
        self.call(self.ADMIN_STEPBUILD_METHOD)
