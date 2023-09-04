#!/bin/env python

import sys
import os
import base_cmd
import fs_util_delegate
import build_rule_config
import build_app_config
from common_define import *
from generation_manager import GenerationManager


class JobCmdBase(base_cmd.BaseCmd):
    def __init__(self):
        super(JobCmdBase, self).__init__()

    def addOptions(self):
        self.parser.add_option('-n', '--clustername', action='store', dest='clusterName')
        self.parser.add_option('-j', '--jobtype', type="choice", action='store', dest='jobType',
                               choices=['hadoop', 'apsara', 'local'])
        self.parser.add_option('-c', '--config', action='store', dest='configPath')
        self.parser.add_option('-i', '--indexroot', action='store', dest='indexRoot')
        self.parser.add_option('', '--username', action='store', dest='userName')
        self.parser.add_option('', '--servicename', action='store', dest='serviceName')
        self.parser.add_option('', '--amonitorport', action='store', dest='amonitorPort')
        self.parser.add_option('', '--step', type="choice", action='store', dest='step',
                               choices=['build', 'merge', 'end_merge', 'both'], default='both')
        self.parser.add_option('', '--retryTimes', type="int", action='store',
                               dest='retryTimes')
        self.parser.add_option('', '--retryInterval', type="int", action='store',
                               dest='retryInterval')
        self.parser.add_option('', '--retryDuration', type="int", action='store',
                               dest='retryDuration')
        self.parser.add_option('', '--tablet', action='store_true', default=False, dest='tabletMode')

    def checkOptionsValidity(self, options):
        if options.clusterName is None:
            raise Exception("cluster_name[-n] must be specified")
        if options.configPath is None:
            raise Exception("config_path[-c] must be specified")
        if not self.fsUtil.exists(options.configPath):
            raise Exception("config_path[%s] not exist." % options.configPath)

    def initMember(self, options):
        super(JobCmdBase, self).initMember(options)
        if options.retryTimes is None:
            self.toolsConf.retryTimes = options.retryTimes
        if options.retryInterval is None:
            self.toolsConf.retryInterval = options.retryInterval
        if options.retryDuration is None:
            self.toolsConf.retryDuration = options.retryDuration
        # recreate fsutil
        self.fsUtil = fs_util_delegate.FsUtilDelegate(self.toolsConf)
        self.clusterName = options.clusterName
        lastConfig = self.getLatestConfig(options.configPath)
        self.buildRuleConf = build_rule_config.BuildRuleConfig(
            self.fsUtil, self.clusterName, lastConfig)
        self.buildAppConf = build_app_config.BuildAppConfig(self.fsUtil, lastConfig)
        if options.indexRoot:
            self.buildAppConf.indexRoot = options.indexRoot
        if options.userName:
            self.buildAppConf.userName = options.userName
        if options.serviceName:
            self.buildAppConf.serviceName = options.serviceName
        if options.amonitorPort:
            self.buildAppConf.amonitorPort = options.amonitorPort
        self.step = options.step
        self.tabletMode = options.tabletMode
        self.delegate = self.buildDelegate(options.jobType, lastConfig)

    def buildDelegate(self, jobType, lastConfig):
        delegateType = None
        if jobType == "hadoop":
            hadoopHome = self.toolsConf.hadoop_home
            if not hadoopHome:
                raise Exception("no hadoop home config in tools.conf")
            import hadoop_job_delegate
            delegateType = hadoop_job_delegate.HadoopJobDelegate
        elif jobType == "apsara":
            import apsara_job_delegate
            delegateType = apsara_job_delegate.ApsaraJobDelegate
        elif jobType == "local":
            import local_job_delegate
            delegateType = local_job_delegate.LocalJobDelegate
        else:
            raise Exception("Unknown job type[%s]" % jobType)
        return delegateType(lastConfig, self.buildAppConf,
                            self.buildRuleConf, self.toolsConf,
                            self.fsUtil, self.clusterName, self.tabletMode)


class StartJobCmd(JobCmdBase):
    '''
    bs {startjob|stj}
        {-j job_type        | --jobtype=job_type}
        {-c config_path     | --config=config_path}
        {-n cluster_name    | --clustername=cluster_name}
        {-m build_mode      | --buildmode=build_mode}
        {-d data_path       | --datapath=data_path}
        {-i index_root      | --indexroot=index_root}
        {-p partition_count | --partcount=partition_count}
        {-g generation_id   | --generationid=generation_id}
        {                   | --documentformat=document_format}
        {                   | --rawdocschemaname=raw_doc_schema_name}
        {                   | --data_descrition=data_description}
        [                   | --gm=generation_meta]
        [                     --pf=partition_id_from]
        [                     --pc=build_partition_count]
        [                     --mergeconfig=merge_config_name]
        [                     --read_src=read_src]
        [                     --timeout]

Option:
    -c config_path, --config=config_path           : required
    -n cluser_name, --clustername=cluster_name     : required
    -j job_type, --jobtype=job_type                : required, hadoop, apsara or local
    -m build_mode, --buildmode=build_mode          : required, either full or incremental,
    -d data_path, --datapath=data_path             : optional, if not specified, read from config
    -i index_root, --indexroot=index_root          : optional, if not specified, read from config
    -p partition_count, --partcount=part_count     : optional, if not specified, read from config
    -g generation_id, --generationid=generation_id : optional, if not specified, scan offline index root
    --documentformat=document_format               : optional, ha3 or isearch, default ha3
    --rawdocschemaname=raw_doc_schema_name         : optional, raw doc schemaname
    --datadescription=data_description             : optional, datadescription
    --gm=generation_meta                           : optional, specified generation meta for build.
    --pf=partition_id_from                         : optional, default is 0
    --pc=build_partition_count                     : optional, must be specified when --pf is specified.
                                                     default is equal to (partition_count)
    --mergeconfig=merge_config_name                : optional, merge config for this job, default:
                                                         full for full job
                                                         inc  for inc  job
    --timeout                                      : optional, there is no timeout by default
    --sleepInterval=sleepInterval                  : optional, sleep interval for get job status, default is 5 seconds
    --parallel=parallel_build_num                  : optional, default is 1,
                                                     can only be used for full build mode.
    --partSplit=partition_split_num                : optional, default is 1,
                                                     can only be used for full build mode.
    --mergeParallelNum=merge_parallel_num          : optional, default is 1,
                                                     can only be used for full build mode.
    --readSrc=read_src                             : required when data_path is empty, read_src in data_table config
    --parameters=parameters                        : optional, additional parameters for starting build job
    -l logConfigPath, --logConfigPath=logConfigPath: optional, specify log config file path

Example:
    bs stj -c config/ -n simple -m full -j apsara
    '''

    def __init__(self):
        super(StartJobCmd, self).__init__()

    def addOptions(self):
        super(StartJobCmd, self).addOptions()
        self.parser.add_option('-m', '--buildmode', type='choice', action='store',
                               dest='buildMode', choices=['full', 'incremental'],
                               default='full')
        self.parser.add_option('-d', '--datapath', action='store', dest='dataPath')
        self.parser.add_option('-p', '--partcount', type="int", action='store',
                               dest='partitionCount')
        self.parser.add_option('-g', '--generationid', type='int',
                               action='store', dest='generationId')
        self.parser.add_option('', '--gm', action='store', dest='generationMeta')
        self.parser.add_option('', '--pf', type='int', action='store', dest='buildPartFrom')
        self.parser.add_option('', '--pc', type='int', action='store', dest='buildPartCount')
        self.parser.add_option('-w', '--work_dir', action='store', dest='workDir')
        self.parser.add_option('', '--mergeconfig', action='store', dest='mergeConfigName',
                               default='')
        self.parser.add_option('', '--timeout', type='int', action='store',
                               dest='timeout', default=sys.maxsize)
        self.parser.add_option('', '--sleepInterval', type='int', action='store',
                               dest='sleepInterval', default=5)
        self.parser.add_option('', '--parallel', type="int", action='store',
                               dest='parallel')
        self.parser.add_option('', '--partSplit', type="int", action='store',
                               dest='partSplit')
        self.parser.add_option('', '--mergeParallelNum', type="int", action='store',
                               dest='mergeParallelNum')
        self.parser.add_option('', '--documentformat', type="choice", action='store',
                               dest='documentFormat', choices=['isearch', 'ha3'])
        self.parser.add_option('', '--rawdocschemaname', action='store',
                               dest='rawDocSchemaName')
        self.parser.add_option('', '--datadescription', action='store',
                               dest='dataDescription')
        self.parser.add_option('', '--readSrc', action='store', dest='readSrc')
        self.parser.add_option('', '--parameters', action='store', dest='parameters',
                               default='')
        self.parser.add_option('-l', '--logConfigPath', action='store', dest='logConfigPath')

    def checkOptionsValidity(self, options):
        super(StartJobCmd, self).checkOptionsValidity(options)

        if options.timeout <= 0:
            raise Exception("invalid value of timeout[%s], please make sure timeout > 0"
                            % options.timeout)

        if options.sleepInterval <= 0:
            raise Exception("invalid value of sleepInterval[%s], please make sure sleepInterval > 0"
                            % str(options.sleepInterval))

        if options.sleepInterval >= options.timeout:
            raise Exception("sleepInterval[%s] should be less than timeout[%s]"
                            % (str(options.sleepInterval), str(options.timeout)))

        # buildStep is guranteed to be JOB_BUILD_STEP when BUILD_MODE_INC
        # inc merge is supposed to be executed in inc build
        if options.buildMode == BUILD_MODE_INC:
            if options.step not in [JOB_BUILD_STEP, JOB_BOTH_STEP]:
                raise Exception('cant %s in %s build' % (options.buildMode, options.buildStep))
            options.step = JOB_BUILD_STEP

        if options.jobType == 'local' and not options.workDir:
            raise Exception('work dir is needed in local mode')

        if options.mergeConfigName:
            # todo: check mergeConfigName
            pass

        if options.generationId is not None:
            GenerationManager.checkGenerationId(options.generationId)

        if (not options.dataPath) and (not options.readSrc) and (not options.dataDescription):
            raise Exception('dataPath and readSrc can not be both empty')

        if options.logConfigPath:
            if not self.fsUtil.exists(options.logConfigPath):
                raise Exception("log config path[%s] not exist." % options.logConfigPath)

    def initMember(self, options):
        super(StartJobCmd, self).initMember(options)
        self.generationMeta = options.generationMeta
        self.timeout = options.timeout
        self.sleepInterval = options.sleepInterval
        self.buildMode = options.buildMode
        self.dataPath = options.dataPath
        self.readSrc = options.readSrc
        self.parameters = options.parameters
        self.logConfigPath = options.logConfigPath
        if self.logConfigPath:
            if self.logConfigPath[0:1] == './' or self.logConfigPath[0:1] == '../':
                self.logConfigPath = os.getcwd() + '/' + self.logConfigPath
        self.documentFormat = options.documentFormat
        self.rawDocSchemaName = options.rawDocSchemaName
        self.dataDescription = options.dataDescription
        if options.partitionCount:
            self.buildRuleConf.partitionCount = options.partitionCount
        if options.parallel:
            self.buildRuleConf.buildParallelNum = options.parallel
        if options.mergeParallelNum:
            self.buildRuleConf.mergeParallelNum = options.mergeParallelNum
        if options.partSplit:
            self.buildRuleConf.partitionSplitNum = options.partSplit
        self.buildRuleConf.buildParallelNum *= self.buildRuleConf.partitionSplitNum
        self.partitionCount = options.partitionCount
        self.buildPartFrom = options.buildPartFrom
        self.buildPartCount = options.buildPartCount
        self.mergeConfigName = options.mergeConfigName
        self.generationId = options.generationId
        if options.workDir:
            os.chdir(options.workDir)

    def _checkBuildPartCount(self):
        if ((self.buildPartFrom is None and self.buildPartCount is not None) or
                (self.buildPartFrom is not None and self.buildPartCount is None)):
            raise Exception('--pf and --pc must both be specified!')

        if self.buildPartFrom is None and self.buildPartCount is None:
            return

        if self.buildPartFrom < 0 or self.buildPartFrom >= self.buildRuleConf.partitionCount:
            raise Exception('range partFrom [%s] must be [%s, %s] ' %
                            (self.buildPartFrom, 0, self.buildRuleConf.partitionCount - 1))

        if self.buildPartCount <= 0 or self.buildPartCount > self.buildRuleConf.partitionCount:
            raise Exception('buildPartCount [%s] must be [%s, %s] ' %
                            (self.buildPartCount, 1, self.buildRuleConf.partitionCount))

        if self.buildPartFrom + self.buildPartCount > self.buildRuleConf.partitionCount:
            raise Exception('partFrom + buildPartCount must <= partCount')

    def run(self):
        self._checkBuildPartCount()
        self.delegate.startJob(self.generationMeta,
                               self.timeout,
                               self.sleepInterval,
                               self.buildMode,
                               self.step,
                               self.buildPartFrom,
                               self.buildPartCount,
                               self.mergeConfigName,
                               self.dataPath,
                               self.generationId,
                               self.documentFormat,
                               self.rawDocSchemaName,
                               self.dataDescription,
                               self.readSrc,
                               self.parameters,
                               self.logConfigPath)


class StopJobCmd(JobCmdBase):
    '''
    bs {stopjob|spj}
        {-j job_type        | --jobtype=job_type}
        {-n cluster_name    | --clustername=cluster_name}
        {-c config_path     | --config=config_path}
        {-i index_root      | --indexroot=index_root}

Option:
    -j job_type, --jobtype=job_type              : required
    -n cluser_name, --clustername=cluster_name   : required
    -c config_path, --config=config_path         : required
    -i index_root, --indexroot=index_root        : optional, if not specified, read from config

Example:
    bs spj -n simple -j hadoop -c config_path
    '''

    def __init__(self):
        super(StopJobCmd, self).__init__()

    def initMember(self, options):
        super(StopJobCmd, self).initMember(options)
        self.jobType = options.jobType

    def checkOptionsValidity(self, options):
        super(StopJobCmd, self).checkOptionsValidity(options)

    def run(self):
        self.delegate.stopJob()


class GetJobStatusCmd(JobCmdBase):
    '''
    bs {getjobstatus|gjs}
        {-j job_type        | --jobtype=job_type}
        {-n cluster_name    | --clustername=cluster_name}
        [-c config_path     | --config=config_path]
        {-i index_root      | --indexroot=index_root}
        [-m build_mode      | --buildmode=build_mode]
        [-v                 | --verbose]

Option:
    -j job_type, --jobtype=job_type              : required, eithor hadoop or apsara
    -n cluser_name, --clustername=cluster_name   : required
    -c config_path, --config=config_path         : required, when jobtype is apsara
    -i index_root, --indexroot=index_root        : optional, if not specified, read from config
    -m build_mode, --buildmode=build_mode        : optional, either full or incremental, default full
    -v , --verbose                               : optional, show job's detail info

Example:
    bs gjs -n simple -j apsara -c /path/to/config/ -v
    bs gjs -n simple -j apsara -c /path/to/config/ -m incremental -v
    '''

    def __init__(self):
        super(GetJobStatusCmd, self).__init__()

    def addOptions(self):
        super(GetJobStatusCmd, self).addOptions()
        self.parser.add_option('-m', '--buildmode', type="choice", action='store',
                               dest='buildMode', choices=['full', 'incremental'],
                               default='full')
        self.parser.add_option('-v', '--verbose', action='store_true',
                               dest='showDetail', default=False)

    def checkOptionsValidity(self, options):
        super(GetJobStatusCmd, self).checkOptionsValidity(options)
        if options.jobType == JOB_TYPE_LOCAL:
            raise Exception("local job not support gbs")

    def initMember(self, options):
        super(GetJobStatusCmd, self).initMember(options)
        self.showDetail = options.showDetail
        self.buildMode = options.buildMode

    def run(self):
        self.delegate.getJobStatus(self.step, self.buildMode, self.showDetail)
