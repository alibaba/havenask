import os

import base_cmd
import swift_common_define
import swift_admin_delegate
import status_analyzer
import swift_util
from swift.protocol.Common_pb2 import *
from swift.protocol.AdminRequestResponse_pb2 import *
import transport_cmd_base
import time
import swift_broker_delegate
import local_file_util
import zlib
import json


class TopicAddCmd(base_cmd.BaseCmd):
    '''
    swift {at|addtopic}
       {-z zookeeper_address           | --zookeeper=zookeeper_address }
       {-t topic_name                  | --topic=topic_name }
       {-c partition_count             | --partcount=partition_count }
       {-R range_count                 | --rangecount=range_count }
       {-s partition_buf_size          | --partbuf=partition_buf_size }
       {-b partition_file_buf_size     | --partfilebuf=partition_file_buf_size }
       {-y partition_max_buf_size      | --partmaxbuf=partition_max_buf_size }
       {-x partition_min_buf_size      | --partminbuf=partition_min_buf_size }
       {-r partition_resource          | --partresource=partition_resource }
       {-l partition_limit             | --partlimit=partition_limit }
       {-m topic_mode                  | --topicmode=topic_mode }
       {-f                             | --fieldfilter }
       {-S                             | --needSchema }
       {-i obsolete_file_interval      | --obsoletefileinterval=obsolete_file_interval }
       {-n reserved_file_count         | --reservedfilecount=reserved_file_count }
       {-d                             | --deletetopicdata }
       {-o security_commit_time        | --committime=security_commit_time }
       {-p security_commit_data        | --commitdata=security_commit_data }
       {-O owners                      | --owners=owners }
       {-q                             | --compress=compress_single_msg }
       {-u                             | --compressthres=compress_single_msg_threshold }
       {-a                             | --dfsroot=dfs_root }
       {-e                             | --extenddfsroot=extend_dfs_root }
       {-g                             | --topicgroup=topic_group }
       {-j                             | --expired=expired }
       {-E                             | --sealed=sealed }
       {-Y                             | --topicType=topic_type }
       {-P                             | --physicTopicLst=physic_topic_list}
       {-N                             | --enableTTLDel=enable_ttl_del}
       {-G                             | --enableLongPolling=enable_long_polling}
       {-v                             | --versionControl=version_control}
       {-M                             | --enableMergeData=enable_merge_data}
       {-I                             | --permitUser=permitUser}
       {-Z                             | --readnotcommitmsg=read_notcommit_msg}

    options:
       -z zookeeper_address,      --zookeeper=zookeeper_address         : required, zookeeper root address
       -t topic_name,             --topic=topic_name                    : required, topic name, eg: news
       -c partition_count,        --partcount=partition_count           : required, partition count of the topic
       -R range_count,            --rangecount=range_count              : optional, range count in one partion, default is 4,
       -s partition_buf_size      --partbuf=partition_buf_size          : [deprecated], partition buffer size of the topic, M
       -b partition_file_buf_size --partfilebuf=partition_file_buf_size : [deprecated], partition read file buffer size of the topic, M
       -x partition_min_buf_size  --partmaxbuf=partition_max_buf_size   : [optional], partition max buffer size of the topic, M
       -y partition_max_buf_size  --partminbuf=partition_min_buf_size   : [optional], partition min buffer size of the topic, M
       -r partition_resource      --partresource=partition_resource     : optional, partition resource[1,1000], default is 100
       -l partition_limit         --partlimit=partition_limit           : optional, partition limit of this topic in one broker, default is no limit
       -m topic_mode              --topicmode=topic_mode                : optional, normal_mode | security_mode | memory_only_mode | memory_prefer_mode, default is normal_mode
       -f                         --fieldfilter                         : optional, filter fields in msg if specialized
       -i obsolete_file_interval  --obsoletefileinterval=obsolete_file_interval : optional, obsolete file time interval, unit is hour.
       -n reserved_file_count     --reservedfilecount=reserved_file_count       : optional, reserved file count.
       -d                         --deletetopicdata                     : optional, delete old topic data if specialized
       -o security_commit_time    --committime=security_commit_time     : optional, max wait time for commit message in security_mode
       -p security_commit_data    --commitdata=security_commit_data     : optional, max data size for commit message in security_mode
       -O owners                  --owners=owners                       : optional, topic owners, multi owners seperate by ,
       -S                         --needschema                          : optional, schema topic
 if specialized
       -V schema_versions         --schema_versions                     : optional, topic schema verisons, must exist, versions seperate by ','
       -q                         --compress=compress_single_msg        : optional, compress msg in this topic
       -u                         --compressthres=compress_single_msg_threshold        : optional, compress msg great than threshold [default:2048]
       -a                         --dfsroot=dfs_root                    : optional, dfs_root
       -e                         --extenddfsroot=extend_dfs_root       : optional, extend dfs_root add old data when dfs root changed
       -g                         --topicgroup=topic_group              : optional, topic_group
       -j                         --expired=expired                     : optional, topic expired time, auto delete topic after expired, second
       -T                         --request_time_out=optional           : optional, timeout for request, ms
       -E                         --sealed                              : optional, is sealed topic or not
       -Y                         --topicType=topic_type                : optional, TOPIC_TYPE_NORMAL | TOPIC_TYPE_PHYSIC | TOPIC_TYPE_LOGIC | TOPIC_TYPE_LOGIC_PHYSIC, default is TOPIC_TYPE_NORMAL
       -P                         --physicTopicLst=physic_topic_list    : optional, physic topic list, seperate by ','
       -N                         --enableTTLDel=enable_ttl_del           : optional, enable TTL delete or not
       -G                         --enable_long_polling=enableLongPolling : optional, enable long polling
       -v                         --versionControl=version_control      : optional, True or False
       -M                         --enableMergeData=enable_merge_data   : optional, enable merge data
       -I                         --permitUser=permitUser               : optional, users allowed to operate this topic, seperate by ','
       -Z                         --readnotcommitmsg=read_notcommit_msg : optional, whether topic can read not committed msg, default is true

Example:
    swift at -z zfs://10.250.12.23:1234/root -t news -c 20 -x 10
    swift at -z zfs://10.250.12.23:1234/root -t news -c 20 -y 100 -b 128
    swift at -z zfs://10.250.12.23:1234/root -t news -c 20 -x 10 -y 100 -r 30
    swift at -z zfs://10.250.12.23:1234/root -t news -c 20 -r 30 -m security_mode -f
    swift at -z zfs://10.250.12.23:1234/root -t news -c 20 -r 30 -i 1 -n 5
    swift at -z zfs://10.250.12.23:1234/root -t news -c 20 -r 30 -l 2
    swift at -z zfs://10.250.12.23:1234/root -t news -c 20 -r 30 -l 2 -q
    swift at -z zfs://10.250.12.23:1234/root -t news -c 1 -j 1000 -O Lilei,Hanmeimei
    swift at -z zfs://10.250.12.23:1234/root -t news -c 20 -a hdfs://xxx/1 -e hdfs://xxx/2,hdfs://xxx/3
    swift at -z zfs://10.250.12.23:1234/root -t news -c 20 -s 10 [deprecated]
    swift at -z zfs://10.250.12.23:1234/root -t news --permitUser=Hanmeimei,Lilei
    '''

    def addOptions(self):
        super(TopicAddCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')
        self.parser.add_option('-c', '--partcount', type='int',
                               action='store', dest='partCnt')
        self.parser.add_option('-R', '--rangecount', type='int',
                               action='store', dest='rangeCnt')
        self.parser.add_option('-s', '--partbuf', type='int',
                               action='store', dest='partBuf')
        self.parser.add_option('-b', '--partfilebuf', type='int',
                               action='store', dest='partFileBuf')
        self.parser.add_option('-x', '--partminbuf', type='int',
                               action='store', dest='partMinBuf')
        self.parser.add_option('-y', '--partmaxbuf', type='int',
                               action='store', dest='partMaxBuf')
        self.parser.add_option('-r', '--partresource', type='int',
                               action='store', dest='partResource')
        self.parser.add_option('-l', '--partlimit', type='int',
                               action='store', dest='partLimit')
        self.parser.add_option('-w', '--waittopicready', type='int',
                               action='store', dest='timeout')
        self.parser.add_option(
            '-m',
            '--topicmode',
            type='choice',
            action='store',
            dest='topicMode',
            choices=[
                'normal_mode',
                'security_mode',
                'memory_only_mode',
                'memory_prefer_mode',
                'persist_data_mode'],
            default='normal_mode')
        self.parser.add_option('-f', '--fieldfilter',
                               action='store_true',
                               dest='fieldFilter',
                               default=False)
        self.parser.add_option('-i', '--obsoletefileinterval', type='int',
                               action='store', dest='obsoleteFileInterval')
        self.parser.add_option('-n', '--reservedfilecount', type='int',
                               action='store', dest='reservedFileCount')
        self.parser.add_option('-d', '--deletetopicdata',
                               action='store_true',
                               dest='deleteTopicData',
                               default=False)
        self.parser.add_option('-o', '--committime', type='int',
                               action='store', dest='commitTime')
        self.parser.add_option('-p', '--commitdata', type='int',
                               action='store', dest='commitData')
        self.parser.add_option('-O', '--owners', action='store', dest='owners')
        self.parser.add_option('-q', '--compress',
                               action='store_true',
                               dest='compressMsg',
                               default=False)
        self.parser.add_option('-u', '--compressthres',
                               action='store_true',
                               dest='compressThres')
        self.parser.add_option('-a', '--dfsRoot', action='store',
                               dest='dfsRoot')
        self.parser.add_option('-e', '--extendDfsRoot', action='store',
                               dest='extendDfsRoot')
        self.parser.add_option('-g', '--topicGroup', action='store',
                               dest='topicGroup')
        self.parser.add_option('-j', '--expired', type='int',
                               action='store', dest='expiredTime')
        self.parser.add_option('-S', '--needschema',
                               action='store_true',
                               dest='needSchema',
                               default=False)
        self.parser.add_option('-V', '--schema_versions', action='store', dest='schemaVersions')
        self.parser.add_option('-T', '--request_time_out', type='int',
                               action='store', dest='requestTimeout')
        self.parser.add_option('-E', '--sealed', action='store',
                               dest='sealed', default='false')
        self.parser.add_option('-Y', '--topic_type', type='choice',
                               action='store', dest='topicType',
                               choices=['TOPIC_TYPE_NORMAL', 'TOPIC_TYPE_PHYSIC',
                                        'TOPIC_TYPE_LOGIC', 'TOPIC_TYPE_LOGIC_PHYSIC'],
                               default='TOPIC_TYPE_NORMAL')
        self.parser.add_option('-P', '--physic_topic_list', action='store',
                               dest='physicTopicLst')
        self.parser.add_option('-N', '--enable_ttl_del', action='store',
                               dest='enableTTLDel', default='true')
        self.parser.add_option('-G', '--enable_long_polling', action='store',
                               dest='enableLongPolling', default='false')
        self.parser.add_option('-v', '--version_control', action='store',
                               dest='versionControl', default='false')
        self.parser.add_option('-M', '--enable_merge_data', action='store',
                               dest='enableMergeData', default='false')
        self.parser.add_option('-I', '--permitUser', action='store',
                               dest='permitUser', default='')
        self.parser.add_option('-Z', '--readnotcommitmsg', action='store',
                               dest='readNotCommmitMsg', default='true')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TopicAddCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicName is None:
            errMsg = "ERROR: topic must be specified!"
            return False, errMsg

        if options.partCnt is None:
            errMsg = "ERROR: partition count for topic[%s] must be specified!"\
                     % options.topicName
            return False, errMsg

        if options.partCnt <= 0:
            errMsg = "ERROR: partition count [%d] must greater than zero" \
                     % options.partCnt
            return False, errMsg

        if options.rangeCnt is not None and options.rangeCnt < 1:
            errMsg = "ERROR: range count [%d] must greater than zero" \
                     % options.rangeCnt
            return False, errMsg

        if options.partBuf is not None:
            errMsg = "WARN: partition buffer is deprecated. using partminbuf and partmaxbuf!"

        if options.partBuf is not None and options.partBuf <= 0:
            errMsg = "ERROR: partition buffer [%d] must greater than zero"\
                     % options.partBuf
            return False, errMsg

        if options.partMaxBuf is None and options.partMinBuf is None and options.partBuf is not None:
            options.partMinBuf = options.partBuf / 2
            if options.partMinBuf == 0:
                options.partMinBuf = 1
            options.partMaxBuf = options.partBuf * 2
            errMsg += " WARN: partmaxbuf and partminbuf is not setted, using partbuf value to"\
                      "set partminbuf %d and partmaxbuf %d!"\
                      % (options.partMinBuf, options.partMaxBuf)

        if options.partMaxBuf is not None and options.partMaxBuf <= 0:
            errMsg = "ERROR: partition max buffer [%d] must greater than zero"\
                     % options.partMaxBuf
            return False, errMsg

        if options.partMinBuf is not None and options.partMinBuf <= 0:
            errMsg = "ERROR: partition min buffer [%d] must greater than zero"\
                     % options.partMinBuf
            return False, errMsg

        if options.partMinBuf is not None and options.partMaxBuf is not None and options.partMinBuf > options.partMaxBuf:
            errMsg = "ERROR: partition min buffer [%d] must not greater than partition"\
                     " max buff [%d]" % (options.partMinBuf, options.partMaxBuf)
            return False, errMsg

        if options.partResource is not None and options.partResource <= 0:
            errMsg = "ERROR: partition resource [%d] must greater than zero" \
                     % options.partResource
            return False, errMsg

        if options.partLimit is not None and options.partLimit <= 0:
            errMsg = "ERROR: partition limit [%d] must greater than zero" % (
                options.partLimit)
            return False, errMsg

        if options.obsoleteFileInterval is not None and options.obsoleteFileInterval <= 0:
            errMsg = "ERROR: obsolete file time interval [%d] must greater than zero" % (
                options.obsoleteFileInterval)
            return False, errMsg

        if options.reservedFileCount is not None and options.reservedFileCount <= 0:
            errMsg = "ERROR: reserved file count [%d] must greater than zero" % (
                options.reservedFileCount)
            return False, errMsg

        if options.partFileBuf is not None:
            errMsg += " WARN: setting partition file buffer has deprecated."

        if options.commitData is not None and options.commitData < 0:
            errMsg = "ERROR: security max commit data should not less than zero [%d]." \
                     % options.commitData
            return False, errMsg

        if options.commitTime is not None and options.commitTime < 0:
            errMsg = "ERROR: security max commit time should not less than zero [%d]." \
                     % options.commitTime
            return False, errMsg

        if options.expiredTime is not None and options.expiredTime < -1:
            errMsg = "ERROR: expired time should not less than -1 [%d]." \
                     % options.commitTime
            return False, errMsg

        if options.fieldFilter is not None and options.fieldFilter \
           and options.needSchema is not None and options.needSchema:
            errMsg = "ERROR: cannot set both field filter and need schema"
            return False, errMsg

        if options.requestTimeout is not None and options.requestTimeout < 0:
            errMsg = "ERROR: expired time should larger than 0[%d]." \
                     % options.requestTimeout
            return False, errMsg

        if options.sealed is not None and options.sealed not in ['true', 'false']:
            errMsg = "ERROR: sealed can only set ['true', 'false']"
            return False, errMsg

        if options.enableTTLDel is not None and options.enableTTLDel not in ['true', 'false']:
            errMsg = "ERROR: enableTTLDel can only set ['true', 'false']"
            return False, errMsg

        if options.enableLongPolling is not None and options.enableLongPolling not in ['true', 'false']:
            errMsg = "ERROR, enableLongPolling can only set ['true', 'false']"
            return False, errMsg

        if options.enableMergeData is not None and options.enableMergeData not in ['true', 'false']:
            errMsg = "ERROR, enableMergeData can only set ['true', 'false']"
            return False, errMsg

        if options.readNotCommmitMsg is not None and options.readNotCommmitMsg not in ['true', 'false']:
            errMsg = "ERROR, readNotCommmitMsg can only set ['true', 'false']"
            return False, errMsg

        return True, errMsg

    def initMember(self, options):
        super(TopicAddCmd, self).initMember(options)
        self.topicName = options.topicName
        self.partCnt = options.partCnt
        self.rangeCnt = options.rangeCnt
        self.partBuf = options.partBuf
        self.partFileBuf = options.partFileBuf
        self.partMinBuf = options.partMinBuf
        self.partMaxBuf = options.partMaxBuf
        self.partResource = options.partResource
        self.partLimit = options.partLimit
        self.timeout = options.timeout
        self.needFieldFilter = options.fieldFilter
        if options.topicMode == "normal_mode":
            self.topicMode = TOPIC_MODE_NORMAL
        elif options.topicMode == "security_mode":
            self.topicMode = TOPIC_MODE_SECURITY
        elif options.topicMode == "memory_only_mode":
            self.topicMode = TOPIC_MODE_MEMORY_ONLY
        elif options.topicMode == "memory_prefer_mode":
            self.topicMode = TOPIC_MODE_MEMORY_PREFER
        elif options.topicMode == "persist_data_mode":
            self.topicMode = TOPIC_MODE_PERSIST_DATA
        self.obsoleteFileInterval = options.obsoleteFileInterval
        self.reservedFileCount = options.reservedFileCount
        self.deleteTopicData = options.deleteTopicData
        self.commitTime = options.commitTime
        self.commitData = options.commitData
        self.compressMsg = options.compressMsg
        self.compressThres = options.compressThres
        self.dfsRoot = options.dfsRoot
        self.topicGroup = options.topicGroup
        self.extendDfsRoot = options.extendDfsRoot
        self.expiredTime = options.expiredTime
        self.owners = options.owners
        self.needSchema = options.needSchema
        self.schemaVersions = options.schemaVersions
        self.requestTimeout = options.requestTimeout
        self.sealed = True if (options.sealed.lower() == 'true') else False
        if options.topicType == "TOPIC_TYPE_NORMAL":
            self.topicType = TOPIC_TYPE_NORMAL
        elif options.topicType == "TOPIC_TYPE_PHYSIC":
            self.topicType = TOPIC_TYPE_PHYSIC
        elif options.topicType == "TOPIC_TYPE_LOGIC":
            self.topicType = TOPIC_TYPE_LOGIC
        elif options.topicType == "TOPIC_TYPE_LOGIC_PHYSIC":
            self.topicType = TOPIC_TYPE_LOGIC_PHYSIC
        self.physicTopicLst = options.physicTopicLst
        self.enableTTLDel = False if (options.enableTTLDel.lower() == 'false') else True
        self.enableLongPolling = True if (options.enableLongPolling.lower() == 'true') else False
        self.versionControl = True if (options.versionControl.lower() == 'true') else False
        self.enableMergeData = True if (options.enableMergeData.lower() == 'true') else False
        self.permitUser = options.permitUser
        self.readNotCommmitMsg = False if (options.readNotCommmitMsg.lower() == 'false') else True

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.createTopic(
            topicName=self.topicName,
            partCnt=self.partCnt, rangeCnt=self.rangeCnt, partBufSize=self.partBuf,
            partMinBufSize=self.partMinBuf, partMaxBufSize=self.partMaxBuf,
            partResource=self.partResource, partLimit=self.partLimit,
            topicMode=self.topicMode, needFieldFilter=self.needFieldFilter,
            obsoleteFileTimeInterval=self.obsoleteFileInterval,
            reservedFileCount=self.reservedFileCount,
            partFileBufSize=self.partFileBuf,
            deleteTopicData=self.deleteTopicData,
            securityCommitTime=self.commitTime,
            securityCommitData=self.commitData,
            compressMsg=self.compressMsg,
            compressThres=self.compressThres,
            dfsRoot=self.dfsRoot,
            topicGroup=self.topicGroup,
            extendDfsRoot=self.extendDfsRoot,
            expiredTime=self.expiredTime,
            owners=self.owners,
            needSchema=self.needSchema,
            schemaVersions=self.schemaVersions,
            requestTimeout=self.requestTimeout,
            sealed=self.sealed,
            topicType=self.topicType,
            physicTopicLst=self.physicTopicLst,
            enableTTLDel=self.enableTTLDel,
            enableLongPolling=self.enableLongPolling,
            versionControl=self.versionControl,
            enableMergeData=self.enableMergeData,
            permitUser=self.permitUser,
            readNotCommmitMsg=self.readNotCommmitMsg)
        if not ret:
            print "Add topic Failed!"
            return ret, response, 1

        print time.time(), "Add topic success!"
        if self.timeout is not None:
            print time.time(), "wait topic ready."
            return self._waitTopicReady(self.timeout)

        return ret, response, 0

    def _waitTopicReady(self, timeout):
        while timeout > 0:
            if self._isTopicReady(self.topicName):
                print time.time(), "topic [%s] is ready" % self.topicName
                return "", "", 0
            timeout -= 0.1
            time.sleep(0.1)
        return "", "wait topic ready timeout.", -1

    def _isTopicReady(self, topicName):
        ret, response, errMsg = self.adminDelegate.getTopicInfo(topicName)
        if ret:
            if response.HasField(swift_common_define.PROTO_TOPIC_INFO):
                topicInfo = response.topicInfo
                for partitionInfo in topicInfo.partitionInfos:
                    if partitionInfo.status != PARTITION_STATUS_RUNNING:
                        print "ERROR: partition info status: %s, response: %s" % \
                            (str(partitionInfo), str(response))
                        return False
            else:
                print "ERROR: response does not have topicInfo field! response: %s" % str(response)
                return False
        else:
            print "get topic info failed: %s, errorMsg: %s" % (topicName, errMsg)
            return False
        return True


class TopicBatchAddCmd(base_cmd.BaseCmd):
    '''
    swift {atb|addtopicbatch}
       {-z zookeeper_address           | --zookeeper=zookeeper_address }
       {-t topic_name                  | --topic=topic_name }
       {-c partition_count             | --partcount=partition_count }
       {-I                             | --permitUsers=permitUsers}
    options:
       -z zookeeper_address,      --zookeeper=zookeeper_address         : required, zookeeper root address
       -t topic_names,            --topic=topic_names                    : required, topic name, eg: news,old
       -c partition_count,        --partcount=partition_count           : required, partition count of the topic
       -I permitUsers,            --permitUsers=permitUsers             : optional, define same as addtopic, seperate by \\;
Example:
    swift at -z zfs://10.250.12.23:1234/root -t news\\;old -c 20\\;3 -x 10\\;20 -O wls,sdd,xieq\\;wls
    separator is ; and do not forget escape \\ when needed
    '''

    def addOptions(self):
        super(TopicBatchAddCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicNames')
        self.parser.add_option('-c', '--partcount', action='store', dest='partCnt')
        self.parser.add_option('-R', '--rangecount', action='store', dest='rangeCnt')
        self.parser.add_option('-b', '--partfilebuf', action='store',
                               dest='partFileBuf')
        self.parser.add_option('-x', '--partminbuf', action='store', dest='partMinBuf')
        self.parser.add_option('-y', '--partmaxbuf', action='store', dest='partMaxBuf')
        self.parser.add_option('-r', '--partresource', action='store', dest='partResource')
        self.parser.add_option('-l', '--partlimit', action='store', dest='partLimit')
        self.parser.add_option('-w', '--waittopicready', type='int',
                               action='store', dest='timeout')
        self.parser.add_option('-m', '--topicmode',
                               action='store', dest='topicMode')
        # choices=['normal_mode', 'security_mode', 'memory_only_mode', 'memory_prefer_mode']
        self.parser.add_option('-f', '--fieldfilter',
                               action='store',
                               dest='fieldFilter')
        # default=False
        self.parser.add_option('-i', '--obsoletefileinterval',
                               action='store', dest='obsoleteFileInterval')
        self.parser.add_option('-n', '--reservedfilecount',
                               action='store', dest='reservedFileCount')
        self.parser.add_option('-d', '--deletetopicdata',
                               action='store',
                               dest='deleteTopicData')
        # default=False
        self.parser.add_option('-o', '--committime',
                               action='store', dest='commitTime')
        self.parser.add_option('-p', '--commitdata',
                               action='store', dest='commitData')
        self.parser.add_option('-O', '--owners', action='store', dest='owners')
        self.parser.add_option('-q', '--compress',
                               action='store',
                               dest='compressMsg')
        # default=False
        self.parser.add_option('-u', '--compressthres',
                               action='store',
                               dest='compressThres')
        self.parser.add_option('-a', '--dfsRoot', action='store',
                               dest='dfsRoot')
        self.parser.add_option('-e', '--extendDfsRoot', action='store',
                               dest='extendDfsRoot')
        self.parser.add_option('-g', '--topicGroup', action='store',
                               dest='topicGroup')
        self.parser.add_option('-j', '--expired',
                               action='store', dest='expiredTime')
        self.parser.add_option('-I', "--permitUsers", action="store", dest="permitUsers")
        self.parser.add_option('-Z', '--readnotcommitmsg', action='store', dest='readNotCommmitMsg')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TopicBatchAddCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicNames is None:
            errMsg = "ERROR: topic names must be specified!"
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(TopicBatchAddCmd, self).initMember(options)
        self.topicNames = options.topicNames
        self.partCnt = options.partCnt
        self.rangeCnt = options.rangeCnt
        self.partFileBuf = options.partFileBuf
        self.partMinBuf = options.partMinBuf
        self.partMaxBuf = options.partMaxBuf
        self.partResource = options.partResource
        self.partLimit = options.partLimit
        self.timeout = options.timeout
        self.needFieldFilter = options.fieldFilter
        self.topicMode = options.topicMode
        self.obsoleteFileInterval = options.obsoleteFileInterval
        self.reservedFileCount = options.reservedFileCount
        self.deleteTopicData = options.deleteTopicData
        self.commitTime = options.commitTime
        self.commitData = options.commitData
        self.compressMsg = options.compressMsg
        self.compressThres = options.compressThres
        self.dfsRoot = options.dfsRoot
        self.topicGroup = options.topicGroup
        self.extendDfsRoot = options.extendDfsRoot
        self.expiredTime = options.expiredTime
        self.owners = options.owners
        self.permitUsers = options.permitUsers
        self.readNotCommmitMsg = options.readNotCommmitMsg

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.createTopicBatch(
            topicNames=self.topicNames,
            partCnt=self.partCnt, rangeCnt=self.rangeCnt,
            partMinBufSize=self.partMinBuf, partMaxBufSize=self.partMaxBuf,
            partResource=self.partResource, partLimit=self.partLimit,
            topicMode=self.topicMode, needFieldFilter=self.needFieldFilter,
            obsoleteFileTimeInterval=self.obsoleteFileInterval,
            reservedFileCount=self.reservedFileCount,
            partFileBufSize=self.partFileBuf,
            deleteTopicData=self.deleteTopicData,
            securityCommitTime=self.commitTime,
            securityCommitData=self.commitData,
            compressMsg=self.compressMsg,
            compressThres=self.compressThres,
            dfsRoot=self.dfsRoot,
            topicGroup=self.topicGroup,
            extendDfsRoot=self.extendDfsRoot,
            expiredTime=self.expiredTime,
            owners=self.owners,
            permitUsers=self.permitUsers,
            readNotCommmitMsg=self.readNotCommmitMsg)
        if not ret:
            print "Add topic Failed!"
            return ret, response, 1

        print "Add topic success!"
        if self.timeout is not None:
            print "wait topic ready."
            return self._waitTopicReady(self.timeout)

        return ret, response, 0

    def _waitTopicReady(self, timeout):
        topicLst = self.topicNames.split(';')
        while timeout > 0:
            readyCount = 0
            for topic in topicLst:
                if self._isTopicReady(topic):
                    print "topic [%s] is ready" % topic
                    readyCount += 1
                else:
                    print "topic [%s] is not ready" % topic
                if readyCount == len(topicLst):
                    return "", "", 0
            timeout -= 0.1
            time.sleep(0.1)
        return "", "wait topic ready timeout.", -1

    def _isTopicReady(self, topicName):
        ret, response, errMsg = self.adminDelegate.getTopicInfo(topicName)
        if ret:
            if response.HasField(swift_common_define.PROTO_TOPIC_INFO):
                topicInfo = response.topicInfo
                for partitionInfo in topicInfo.partitionInfos:
                    if partitionInfo.status != PARTITION_STATUS_RUNNING:
                        print "ERROR: partition info status: %s, response: %s" % \
                            (str(partitionInfo), str(response))
                        return False
            else:
                print "ERROR: response does not have topicInfo field! response: %s" % str(response)
                return False
        else:
            print "get topic info failed: %s, errorMsg: %s" % (topicName, errMsg)
            return False
        return True


class TopicModifyCmd(base_cmd.BaseCmd):
    '''
    swift {mt|modifytopic}
       {-z zookeeper_address           | --zookeeper=zookeeper_address }
       {-t topic_name                  | --topic=topic_name }
       {-r partition_resource          | --partresource=partition_resource }
       {-l partition_limit             | --partlimit=partition_limit }
       {-g topic_group                 | --group=topic_group }
       {-j expired_time                | --expired_time=expired_time }
       {-m topic_mode                  | --topicmode=topic_mode }
       {-c partition_count             | --partcount=partition_count }
       {-C partition_range_count       | --partrangecount=partition_range_count }
       {-y partition_max_buf_size      | --partmaxbuf=partition_max_buf_size }
       {-x partition_min_buf_size      | --partminbuf=partition_min_buf_size }
       {-i obsolete_file_interval      | --obsoletefileinterval=obsolete_file_interval }
       {-n reserved_file_count         | --reservedfilecount=reserved_file_count }
       {-o security_commit_time        | --committime=security_commit_time }
       {-p security_commit_data        | --commitdata=security_commit_data }
       {-O owners                      | --owners=owners }
       {-a                             | --dfsroot=dfs_root }
       {-e                             | --extenddfsroot=extend_dfs_root }
       {-E                             | --sealed=sealed }
       {-Y                             | --topicType=topic_type }
       {-P                             | --physicTopicLst=physic_topic_list}
       {-N                             | --enableTTLDel=enable_ttl_del}
       {-L                             | --readSizeLimitSec=read_size_limit_sec}
       {-G                             | --enableLongPolling=enable_long_polling}
       {-M                             | --enableMergeData=enable_merge_data}
       {-I permitUser                  | --permitUser=permitUser }
       {-v version_controller          | --versionControl=version_control }
       {-Z                             | --readnotcommitmsg=read_notcommit_msg}
    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address     : required, zookeeper root address
       -t topic_names,        --topic=topic_names               : required, topic name, eg: news,news1
       -f topic_file,         --topic_file=topic_file           : optional, topic file, will append in topic_names, one topic per line
       -r partition_resource  --partresource=partition_resource : optional, partition resource
       -l partition_limit     --partlimit=partition_limit       : optional, partition limit of this topic in one broker
       -g topic_group         --group=topic_group               : optional, change topic group name
       -j expired_time        --expired_time=expired_time       : optional, change expired time for topic, -1 no expired,  second
       -m topic_mode              --topicmode=topic_mode                : optional, normal_mode | security_mode | memory_only_mode | memory_prefer_mode, default is normal_mode
       -c partition_count,        --partcount=partition_count           : optional, partition count of the topic
       -C partition_range_count,  --partrangecount=partition_range_count  : optional, partition range count of the topic
       -x partition_min_buf_size  --partmaxbuf=partition_max_buf_size   : [optional], partition max buffer size of the topic, M
       -y partition_max_buf_size  --partminbuf=partition_min_buf_size   : [optional], partition min buffer size of the topic, M
       -i obsolete_file_interval  --obsoletefileinterval=obsolete_file_interval : optional, obsolete file time interval, unit is hour.
       -n reserved_file_count     --reservedfilecount=reserved_file_count       : optional, reserved file count
       -o security_commit_time    --committime=security_commit_time     : optional, max wait time for commit message in security_mode
       -p security_commit_data    --commitdata=security_commit_data     : optional, max data size for commit message in security_mode
       -O owners                  --owners=owners                       : optional, topic owners, multi owners seperate by ,
       -E                         --sealed                              : optional, is sealed topic or not
       -Y                         --topicType=topic_type                : optional, TOPIC_TYPE_NORMAL | TOPIC_TYPE_PHYSIC | TOPIC_TYPE_LOGIC | TOPIC_TYPE_LOGIC_PHYSIC, default is TOPIC_TYPE_NORMAL
       -P                         --physicTopicLst=physic_topic_list    : optional, physic topic list, seperate by ','
       -N                         --enableTTLDel=enable_ttl_del           : optional, enable TTL delete or not
       -L                         --readSizeLimitSec=read_size_limit_sec  : optional, read dfs limit one sec
       -G                         --enable_long_polling=enableLongPolling : optional, enable long polling
       -M                         --enable_merge_data=enableMergeData : optional, enable merge data
       -I permitUser              --permitUser=permitUser               : optional, users allowed to operate this topic, seperate by ','
       -v version_control         --versionControl=version_control      : optional, writer version controller
       -Z                         --readnotcommitmsg=read_notcommit_msg : optional, whether topic can read not committed msg
Example:
    swift mt -z zfs://10.250.12.23:1234/root -t news -r 10
    swift mt -z zfs://10.250.12.23:1234/root -t news -l 2
    swift mt -z zfs://10.250.12.23:1234/root -t news -r 20 -l 3
    swift mt -z zfs://10.250.12.23:1234/root -t news -g group_name
    swift mt -z zfs://10.250.12.23:1234/root -t news -j -1
    swift mt -z zfs://10.250.12.23:1234/root -t news -m memory_only_mode
    swift mt -z zfs://10.250.12.23:1234/root -t news --permitUser=Hanmeimei,Lilei
    '''

    def addOptions(self):
        super(TopicModifyCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')
        self.parser.add_option('-f', '--topic_file', action='store', dest='topicFile')
        self.parser.add_option('-r', '--partresource', type='int',
                               action='store', dest='partResource')
        self.parser.add_option('-l', '--partlimit', type='int',
                               action='store', dest='partLimit')
        self.parser.add_option('-g', '--group', action='store', dest='topicGroup')
        self.parser.add_option('-j', '--expired_time', type='int',
                               action='store', dest='expiredTime')
        self.parser.add_option('-x', '--partminbuf', type='int',
                               action='store', dest='partMinBuf')
        self.parser.add_option('-y', '--partmaxbuf', type='int',
                               action='store', dest='partMaxBuf')
        self.parser.add_option(
            '-m',
            '--topicmode',
            type='choice',
            action='store',
            dest='topicMode',
            choices=[
                'normal_mode',
                'security_mode',
                'memory_only_mode',
                'memory_prefer_mode',
                'persist_data_mode'])
        self.parser.add_option('-c', '--partcount', type='int',
                               action='store', dest='partCnt')
        self.parser.add_option('-C', '--partrangecount', type='int',
                               action='store', dest='partRangeCnt')
        self.parser.add_option('-i', '--obsoletefileinterval', type='int',
                               action='store', dest='obsoleteFileInterval')
        self.parser.add_option('-n', '--reservedfilecount', type='int',
                               action='store', dest='reservedFileCount')
        self.parser.add_option('-o', '--committime', type='int',
                               action='store', dest='commitTime')
        self.parser.add_option('-p', '--commitdata', type='int',
                               action='store', dest='commitData')
        self.parser.add_option('-O', '--owners', action='store', dest='owners')
        self.parser.add_option('-a', '--dfsRoot', action='store',
                               dest='dfsRoot')
        self.parser.add_option('-e', '--extendDfsRoot', action='store',
                               dest='extendDfsRoot')
        self.parser.add_option('-E', '--sealed', action='store', dest='sealed')
        self.parser.add_option('-Y', '--topic_type', type='choice',
                               action='store', dest='topicType',
                               choices=['TOPIC_TYPE_NORMAL', 'TOPIC_TYPE_PHYSIC',
                                        'TOPIC_TYPE_LOGIC', 'TOPIC_TYPE_LOGIC_PHYSIC'])
        self.parser.add_option('-P', '--physic_topic_list', action='store',
                               dest='physicTopicLst')
        self.parser.add_option('-N', '--enable_ttl_del', action='store',
                               dest='enableTTLDel')
        self.parser.add_option('-L', '--read_size_limit_sec', type='int',
                               action='store', dest='readSizeLimitSec')
        self.parser.add_option('-G', '--enable_long_polling', action='store',
                               dest='enableLongPolling')
        self.parser.add_option('-M', '--enable_merge_data', action='store',
                               dest='enableMergeData')
        self.parser.add_option('-I', '--permitUser', action='store',
                               dest='permitUser', default='')
        self.parser.add_option('-v', '--versionControl', action='store',
                               dest='versionControl')
        self.parser.add_option('-Z', '--readnotcommitmsg', action='store', dest='readNotCommmitMsg')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TopicModifyCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicName is None:
            errMsg = "ERROR: topic must be specified!"
            return False, errMsg

        if options.partResource is not None and options.partResource <= 0:
            errMsg = "ERROR: partition resource [%d] must greater than zero" \
                % options.partResource
            return False, errMsg

        if options.partLimit is not None and options.partLimit <= 0:
            errMsg = "ERROR: partition limit [%d] must greater than zero" % (
                options.partLimit)
            return False, errMsg

        if options.expiredTime is not None and options.expiredTime < -1:
            errMsg = "ERROR: expired time should not less than -1 [%d]." \
                     % options.commitTime
            return False, errMsg

        if options.partMaxBuf is not None and options.partMaxBuf <= 0:
            errMsg = "ERROR: partition max buffer [%d] must greater than zero"\
                     % options.partMaxBuf
            return False, errMsg

        if options.partCnt is not None and options.partCnt <= 0:
            errMsg = "ERROR: partition count [%d] must greater than zero" \
                     % options.partCnt
            return False, errMsg

        if options.partRangeCnt is not None and options.partRangeCnt <= 0:
            errMsg = "ERROR: partition Range count [%d] must greater than zero" \
                     % options.partRangeCnt
            return False, errMsg

        if options.partMinBuf is not None and options.partMinBuf <= 0:
            errMsg = "ERROR: partition min buffer [%d] must greater than zero"\
                     % options.partMinBuf
            return False, errMsg

        if options.partMinBuf is not None and options.partMaxBuf is not None and options.partMinBuf > options.partMaxBuf:
            errMsg = "ERROR: partition min buffer [%d] must not greater than partition max"\
                     " buff [%d]" % (options.partMinBuf, options.partMaxBuf)
            return False, errMsg

        if options.obsoleteFileInterval is not None and options.obsoleteFileInterval <= 0:
            errMsg = "ERROR: obsolete file time interval [%d] must greater than zero" % (
                options.obsoleteFileInterval)
            return False, errMsg

        if options.reservedFileCount is not None and options.reservedFileCount <= 0:
            errMsg = "ERROR: reserved file count [%d] must greater than zero" % (
                options.reservedFileCount)
            return False, errMsg

        if options.sealed is not None and options.sealed not in ['true', 'false']:
            errMsg = "ERROR: sealed can only set ['true', 'false']"
            return False, errMsg

        if options.enableTTLDel is not None and options.enableTTLDel not in ['true', 'false']:
            errMsg = "ERROR: enableTTLDel can only set ['true', 'false']"
            return False, errMsg

        if options.readSizeLimitSec is not None and options.readSizeLimitSec < 0:
            errMsg = "ERROR: readSizeLimitSec [%d] must not less than zero" % (
                options.readSizeLimitSec)
            return False, errMsg

        if options.enableLongPolling is not None and options.enableLongPolling not in ['true', 'false']:
            errorMsg = "ERROR, enableLongPolling can only set ['true', 'false']"
            return False, errMsg

        if options.enableMergeData is not None and options.enableMergeData not in ['true', 'false']:
            errorMsg = "ERROR, enableMergeData can only set ['true', 'false']"
            return False, errMsg

        if options.commitData is not None and options.commitData < 0:
            errMsg = "ERROR: security max commit data should not less than zero [%d]." \
                     % options.commitData
            return False, errMsg

        if options.commitTime is not None and options.commitTime < 0:
            errMsg = "ERROR: security max commit time should not less than zero [%d]." \
                     % options.commitTime
            return False, errMsg

        if options.readNotCommmitMsg is not None and options.readNotCommmitMsg not in ['true', 'false']:
            errorMsg = "ERROR, readNotCommmitMsg can only set ['true', 'false']"
            return False, errMsg

        if options.partLimit is None and options.partResource is None and \
           options.topicGroup is None and options.expiredTime is None and \
           options.partMaxBuf is None and options.partMinBuf is None and \
           options.topicMode is None and options.obsoleteFileInterval is None and \
           options.reservedFileCount is None and options.partCnt is None and \
           options.commitData is None and options.commitTime is None and \
           options.owners is None and options.dfsRoot is None and \
           options.extendDfsRoot is None and options.partRangeCnt is None and\
           options.sealed is None and options.topicType is None and\
           options.physicTopicLst is None and options.enableTTLDel is None and\
           options.readSizeLimitSec is None and options.enableLongPolling is None and\
           options.enableMergeData is None and options.permitUser is None and options.versionControl is None\
           and options.readNotCommmitMsg is None:
            errMsg = "ERROR: modify options: partlimit, topic group, partresource, "\
                     "expired time, partition_max_buf_size, partition_min_buf_size, "\
                     "obsolete_file_interval, reserved_file_count, dfs_root, "\
                     "extend_dfs_root, range_count in partition, owners "\
                     "security max commit data, security max commit time "\
                     "sealed, topic_type, physic_topic_list or enable_ttl_del "\
                     "read_size_limit_sec, enable_long_polling, enableMergeData, "\
                     "readNotCommmitMsg must specify at least one!"
            return False, errMsg

        return True, ''

    def initMember(self, options):
        super(TopicModifyCmd, self).initMember(options)
        self.topicNames = options.topicName.split(',')
        if (options.topicFile):
            f = open(options.topicFile, 'r')
            for line in f.readlines():
                line = line.strip('\n')
                self.topicNames.append(line)
        self.partResource = options.partResource
        self.partLimit = options.partLimit
        self.topicGroup = options.topicGroup
        self.expiredTime = options.expiredTime
        self.partMinBuf = options.partMinBuf
        self.partMaxBuf = options.partMaxBuf
        self.topicMode = None
        if options.topicMode is not None:
            if options.topicMode == "normal_mode":
                self.topicMode = TOPIC_MODE_NORMAL
            elif options.topicMode == "security_mode":
                self.topicMode = TOPIC_MODE_SECURITY
            elif options.topicMode == "memory_only_mode":
                self.topicMode = TOPIC_MODE_MEMORY_ONLY
            elif options.topicMode == "memory_prefer_mode":
                self.topicMode = TOPIC_MODE_MEMORY_PREFER
            elif options.topicMode == "persist_data_mode":
                self.topicMode = TOPIC_MODE_PERSIST_DATA
        self.obsoleteFileInterval = options.obsoleteFileInterval
        self.reservedFileCount = options.reservedFileCount
        self.partCnt = options.partCnt
        self.partRangeCnt = options.partRangeCnt
        self.owners = options.owners
        self.dfsRoot = options.dfsRoot
        self.extendDfsRoot = options.extendDfsRoot
        self.sealed = None
        if options.sealed is not None:
            self.sealed = True if (options.sealed.lower() == 'true') else False
        self.topicType = None
        if options.topicType is not None:
            if options.topicType == "TOPIC_TYPE_NORMAL":
                self.topicType = TOPIC_TYPE_NORMAL
            elif options.topicType == "TOPIC_TYPE_PHYSIC":
                self.topicType = TOPIC_TYPE_PHYSIC
            elif options.topicType == "TOPIC_TYPE_LOGIC":
                self.topicType = TOPIC_TYPE_LOGIC
            elif options.topicType == "TOPIC_TYPE_LOGIC_PHYSIC":
                self.topicType = TOPIC_TYPE_LOGIC_PHYSIC
        self.physicTopicLst = options.physicTopicLst
        self.enableTTLDel = None
        if options.enableTTLDel is not None:
            self.enableTTLDel = False if (options.enableTTLDel.lower() == 'false') else True
        self.readSizeLimitSec = options.readSizeLimitSec
        self.enableLongPolling = None
        if options.enableLongPolling is not None:
            self.enableLongPolling = True if (options.enableLongPolling.lower() == 'true') else False
        self.enableMergeData = None
        if options.enableMergeData is not None:
            self.enableMergeData = True if (options.enableMergeData.lower() == 'true') else False
        self.commitTime = options.commitTime
        self.commitData = options.commitData
        self.permitUser = options.permitUser
        self.versionControl = None
        if options.versionControl is not None:
            self.versionControl = True if (options.versionControl.lower() == 'true') else False
        self.readNotCommmitMsg = None
        if options.readNotCommmitMsg is not None:
            self.readNotCommmitMsg = False if (options.readNotCommmitMsg.lower() == 'false') else True

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        responses = []
        fail_cnt = 0
        for topicName in self.topicNames:
            topicName = topicName.strip()
            ret, response, errorMsg = self.adminDelegate.modifyTopic(
                topicName, self.partResource, self.partLimit, self.topicGroup,
                self.expiredTime, self.partMinBuf, self.partMaxBuf, self.topicMode,
                self.obsoleteFileInterval, self.reservedFileCount, self.partCnt,
                self.owners, self.dfsRoot, self.extendDfsRoot, self.partRangeCnt,
                self.sealed, self.topicType, self.physicTopicLst, self.enableTTLDel,
                self.readSizeLimitSec, self.enableLongPolling, self.commitTime,
                self.commitData, self.enableMergeData, self.permitUser, self.versionControl, self.readNotCommmitMsg)
            if not ret:
                fail_cnt += 1
                print("Modify topic [%s] failed!" % topicName)
                print(response)
            else:
                print("Modify topic [%s] success!" % topicName)
            responses.append(response)
        error_code = 1 if fail_cnt == len(self.topicNames) else 0
        return "", responses, error_code


class TopicDeleteCmd(base_cmd.BaseCmd):
    '''
    swift {dt|deletetopic}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }
       {-t topic_name          | --topic=topic_name }
       {-f file_name           | --file=file_name }

    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,         --topic=topic_name              : optional, topic name, eg: news
       -f file_name,          --file=file_name                : optional,
 file name

Example:
    swift dt -z zfs://10.250.12.23:1234/root -t news
    swift dt -z zfs://10.250.12.23:1234/root -f file_name
    '''

    def addOptions(self):
        super(TopicDeleteCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')
        self.parser.add_option('-f', '--file', action='store', dest='fileName')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TopicDeleteCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicName is None and options.fileName is None:
            errMsg = "ERROR: topic or file must be specified!"
            return False, errMsg
        if options.topicName is not None and options.fileName is not None:
            errMsg = "ERROR: must specified one of topic and file"
            return False, errMsg

        return True, ''

    def initMember(self, options):
        super(TopicDeleteCmd, self).initMember(options)
        self.topicName = options.topicName
        self.fileName = options.fileName
        self.localFileUtil = local_file_util.LocalFileUtil()
        self.failTopicFile = './fail_topic_list_for_delete'

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        failTopicList = []
        if self.topicName is not None:
            ret, respone, errorMsg = self.adminDelegate.deleteTopic(self.topicName)
            if not ret:
                print "Delete topic failed!"
                print errorMsg
                return ret, respone, 1
            print "Delete topic success!"
            return ret, respone, 0
        elif self.fileName is not None:
            topics = self.localFileUtil.cat(self.fileName)
            topicVec = topics.split('\n')
            for topicName in topicVec:
                ret, respone, errorMsg = self.adminDelegate.deleteTopic(topicName)
                if not ret:
                    print "Delete topic %s failed!" % topicName
                    print errorMsg
                    failTopicList.append(topicName)
                else:
                    print "Delete topic %s success!" % topicName
            if not self.writeFailTopic(failTopicList):
                print "write fail topic to file:fail_topic_list failed"
                return ret, respone, 1
            if len(failTopicList) == 0:
                return ret, respone, 0
            else:
                return ret, respone, 1

    def writeFailTopic(self, failTopicList):
        topicStr = ''
        for failTopic in failTopicList:
            topicStr += failTopic + '\n'
        return self.localFileUtil.write(topicStr, self.failTopicFile, 'w')


class TopicDeleteBatchCmd(base_cmd.BaseCmd):
    '''
    swift {dtb|deletetopicbatch}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }
       {-t topic_names         | --topic=topic_name1,topic_name2,...}
       {-f file_name           | --file=file_name }

    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_names,        --topic=topic_name1,topic_name2,...  : optional, topic name, eg: news,old,test
       -f file_name,          --file=file_name                : optional,
 file name

Example:
    swift dtb -z zfs://10.250.12.23:1234/root -t news,old
    swift dtb -z zfs://10.250.12.23:1234/root -f file_name
    '''

    def addOptions(self):
        super(TopicDeleteBatchCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicNames')
        self.parser.add_option('-f', '--file', action='store', dest='fileName')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TopicDeleteBatchCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicNames is None and options.fileName is None:
            errMsg = "ERROR: topic or file must be specified!"
            return False, errMsg
        if options.topicNames is not None and options.fileName is not None:
            errMsg = "ERROR: must specified one of topic and file"
            return False, errMsg

        return True, ''

    def initMember(self, options):
        super(TopicDeleteBatchCmd, self).initMember(options)
        self.topicNames = options.topicNames
        self.fileName = options.fileName
        self.localFileUtil = local_file_util.LocalFileUtil()
        self.failTopicFile = './fail_topic_list_for_delete'

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        failTopicList = []
        if self.topicNames is not None:
            topicVec = self.topicNames.split(',')
            ret, respone, errorMsg = self.adminDelegate.deleteTopicBatch(topicVec)
            if self.fromApi:
                return ret, respone, errorMsg
            if not ret:
                print "Delete topic failed!"
                print errorMsg
                return "", "", 1
            print "Delete topic %s success!" % str(topicVec)
            return "", "", 0
        elif self.fileName is not None:
            topics = self.localFileUtil.cat(self.fileName)
            topicVec = topics.split('\n')
            ret, respone, errorMsg = self.adminDelegate.deleteTopicBatch(topicVec)
            if self.fromApi:
                return ret, respone, errorMsg
            if not ret:
                print "Delete topic in file failed!"
                print errorMsg
                failTopicList.append(topicVec)
            else:
                print "Delete topic %s success!" % str(topicVec)
            if len(failTopicList) > 0 and not self.writeFailTopic(failTopicList):
                print "write fail topic to file:fail_topic_list failed"
                return "", "", 1
            if len(failTopicList) == 0:
                return "", "", 0
            else:
                return "", "", 1

    def writeFailTopic(self, failTopicList):
        topicStr = ''
        for failTopic in failTopicList:
            topicStr += failTopic + '\n'
        return self.localFileUtil.write(topicStr, self.failTopicFile, 'w')


class TopicNamesCmd(base_cmd.BaseCmd):
    '''
    swift {gtn|gettopicnames}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }
       {-f file_name           | --file=file_name }
    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address
       -f file_name           --file=file_name
Example:
    swift gtn -z zfs://10.250.12.23:1234/root
    swift gtn -z zfs://10.250.12.23:1234/root -f file.txt

    '''

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def addOptions(self):
        super(TopicNamesCmd, self).addOptions()
        self.parser.add_option('-f', '--file', action='store', dest='fileName')

    def initMember(self, options):
        super(TopicNamesCmd, self).initMember(options)
        self.fileName = options.fileName
        self.localFileUtil = local_file_util.LocalFileUtil()

    def writeTopicName(self, topicNames):
        topicStr = ''
        for topicName in topicNames:
            print topicName
            topicStr += topicName + '\n'
        return self.localFileUtil.write(topicStr, self.fileName, 'w')

    def run(self):
        ret, response, errMsg = self.adminDelegate.getAllTopicName()
        if self.fromApi:
            return ret, response, errMsg
        if not ret:
            print "Get all topic name failed! "
            print errMsg
            return "", "", 1
        else:
            print "Get topic names success! "
            if len(response.names) == 0:
                print "Swift system has no topic!"
            else:
                if self.fileName:
                    self.writeTopicName(response.names)
                else:
                    namesStr = ", ".join(response.names)
                    print "Topic names: [%s]" % namesStr
        return "", "", 0


class TopicInfosCmd(base_cmd.BaseCmd):
    '''
    swift {gti|gettopicinfo}
       {-z zookeeper_address   | --zookeeper=zookeeper_address}
       {-t topic_name          | --topic=topic_name}
       [-c cmd_type            | --cmdtype=cmd_type]

    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,         --topic=topic_name              : required, topic name, eg: news
       -c cmd_type,           --cmdtype=cmd_type              : optional, command type, default: summary
          summary:            topic summary infos
          message:            protocol message
          verbose:            topic verbose infos
       -s sortType            --sort=sortType                 : optional, partid(default)|brokeraddress| partstatus
       -g group_name          --group=group_name              : optional, group name when get all topic info
       -T                         --request_time_out=optional           : optional, timeout for request, ms

Example:
    swift gti -z zfs://10.250.12.23:1234/root
    swift gti -z zfs://10.250.12.23:1234/root -t news
    swift gti -z zfs://10.250.12.23:1234/root -t news -c message
    swift gti -z zfs://10.250.12.23:1234/root -t news -c verbose
    swift gti -z zfs://10.250.12.23:1234/root -t news -c verbose -s brokeraddress
    '''

    def addOptions(self):
        super(TopicInfosCmd, self).addOptions()
        self.parser.add_option('-t', '--topic',
                               action='store',
                               dest='topicName')
        self.parser.add_option('-g', '--group',
                               action='store',
                               dest='groupName')
        self.parser.add_option('-c', '--cmdtype',
                               type='choice',
                               action='store',
                               choices=['summary', 'message', 'verbose'],
                               dest='cmdType',
                               default='summary')
        self.parser.add_option('-s', '--sort',
                               type='choice',
                               action='store',
                               dest='sortType',
                               choices=[swift_common_define.PART_SORT_PART_ID,
                                        swift_common_define.PART_SORT_PART_STATUS,
                                        swift_common_define.PART_SORT_BROKER_ADDRESS],
                               default='partid')
        self.parser.add_option('-T', '--optional', type='int',
                               action='store', dest='requestTimeout')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TopicInfosCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicName is None:
            if options.cmdType != 'summary':
                errMsg = "ERROR: getAllTopicInfos method only support summary cmd type!"
                return False, errMsg

        return True, ''

    def initMember(self, options):
        super(TopicInfosCmd, self).initMember(options)
        self.topicName = options.topicName
        self.cmdType = options.cmdType
        self.sortType = options.sortType
        self.groupName = options.groupName
        self.requestTimeout = options.requestTimeout

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil,
            self.zfsAddress,
            self.adminLeader,
            self.options.username,
            self.options.passwd,
            self.options.accessId,
            self.options.accessKey)
        return True

    def printTopicInfo(self):
        ret, response, errMsg = self.adminDelegate.getTopicInfo(self.topicName,
                                                                self.requestTimeout)
        if self.fromApi:
            return ret, response, errMsg
        if not ret:
            print "Get topic info failed!"
            print errMsg
            return "", "", 1

        if not response.HasField(swift_common_define.PROTO_TOPIC_INFO):
            print "ERROR: response does not have topicInfo field!"
            return "", "", 1

        topicInfo = response.topicInfo
        if self.cmdType == "summary" or self.cmdType == 'verbose':
            topicInfoAnalyzer = status_analyzer.TopicInfoAnalyzer()
            partCount = topicInfoAnalyzer.getPartitionCount(topicInfo)
            print "TopicName: %s" % topicInfo.name
            if topicInfo.topicMode == TOPIC_MODE_NORMAL:
                print "TopicMode: normal_mode"
            elif topicInfo.topicMode == TOPIC_MODE_SECURITY:
                print "TopicMode: security_mode"
                print "MaxCommitTime: %d" % topicInfo.maxWaitTimeForSecurityCommit
                print "MaxCommitSize: %d" % topicInfo.maxDataSizeForSecurityCommit
            elif topicInfo.topicMode == TOPIC_MODE_MEMORY_ONLY:
                print "TopicMode: memory_only_mode "
            elif topicInfo.topicMode == TOPIC_MODE_MEMORY_PREFER:
                print "TopicMode: memory_prefer_mode "
            if topicInfo.needFieldFilter:
                print "NeedFieldFilter: true"
            else:
                print "NeedFieldFilter: false"

            if topicInfo.needSchema:
                print "NeedSchema: true"
            else:
                print "NeedSchema: false"
            print "SchemaVersions: %s" % ','.join([str(x) for x in topicInfo.schemaVersions])
            print "TopicStatus: %s" % (
                swift_util.SwiftUtil.protoEnumToString(topicInfo, "status")[13:])
            print "PartitionCount: %d" % topicInfo.partitionCount
            print "RangeCountInPartition: %d" % topicInfo.rangeCountInPartition
            print "PartitionMaxBufferSize: %d" % topicInfo.partitionMaxBufferSize
            print "PartitionMinBufferSize: %d" % topicInfo.partitionMinBufferSize
            print "PartitionBufferSize: %d" % topicInfo.partitionBufferSize
            print "PartitionFileBufferSize: %d" % topicInfo.partitionFileBufferSize
            print "PartitionResource: %d" % topicInfo.resource
            print "PartitionLimit: %d" % topicInfo.partitionLimit
            print "ObsoleteFileTimeInterval: %d" % topicInfo.obsoleteFileTimeInterval
            print "ReservedFileCount: %d" % topicInfo.reservedFileCount
            if topicInfo.deleteTopicData:
                print "DeleteTopicData: true"
            else:
                print "DeleteTopicData: false"
            print "TopicGroup: %s" % topicInfo.topicGroup
            print "TopicExpiredTime: %d" % topicInfo.topicExpiredTime
            if 0 == len(topicInfo.owners):
                print "owners: %s" % 'NONE'
            else:
                print "owners: %s" % ','.join(topicInfo.owners)
            if topicInfo.topicType == TOPIC_TYPE_NORMAL:
                print "TopicType: TOPIC_TYPE_NORMAL"
            elif topicInfo.topicType == TOPIC_TYPE_PHYSIC:
                print "TopicType: TOPIC_TYPE_PHYSIC"
            elif topicInfo.topicType == TOPIC_TYPE_LOGIC:
                print "TopicType: TOPIC_TYPE_LOGIC"
            elif topicInfo.topicType == TOPIC_TYPE_LOGIC_PHYSIC:
                print "TopicType: TOPIC_TYPE_LOGIC_PHYSIC"
            if 0 == len(topicInfo.physicTopicLst):
                print "physicTopicLst: []"
            else:
                print "physicTopicLst: %s" % ','.join(topicInfo.physicTopicLst)
            if topicInfo.sealed:
                print "Sealed: true"
            else:
                print "Sealed: false"
            if topicInfo.enableTTLDel:
                print "enableTTLDel: true"
            else:
                print "enableTTLDel: false"
            print "readSizeLimitSec: %d" % topicInfo.readSizeLimitSec
            if topicInfo.enableLongPolling:
                print "enableLongPolling: true"
            else:
                print "enableLongPolling: false"
            if topicInfo.versionControl:
                print "versionControl: true"
            else:
                print "versionControl: false"
            if topicInfo.enableMergeData:
                print "enableMergeData: true"
            else:
                print "enableMergeData: false"
            if 0 == len(topicInfo.permitUser):
                print "permitUser: NONE"
            else:
                print "permitUser: %s" % ','.join(topicInfo.permitUser)
            if topicInfo.readNotCommittedMsg:
                print "readNotCommittedMsg: true"
            else:
                print "readNotCommittedMsg: false"
            print("Partition(waiting/starting/running/stopping/recovering/none): "
                  "%d/%d/%d/%d/%d/%d ") % (
                partCount.partWaiting,
                partCount.partStarting,
                partCount.partRunning,
                partCount.partStopping,
                partCount.partRecovering,
                partCount.partNone)

            if self.cmdType == "verbose":
                partInfos = topicInfoAnalyzer.getSortedPartitionInfo(
                    topicInfo, self.sortType)
                if len(partInfos) > 0:
                    print ""
                    print "Partition Detailed Infos:"
                    print '%-8s %-13s %-40s' % (
                        swift_common_define.PART_SORT_PART_ID,
                        swift_common_define.PART_SORT_PART_STATUS,
                        swift_common_define.PART_SORT_BROKER_ADDRESS)
                    for partInfo in partInfos:
                        print '%-8d %-13s %-40s' % (
                            partInfo.id,
                            swift_util.SwiftUtil.protoEnumToString(partInfo, "status")[17:],
                            partInfo.brokerAddress)
        elif self.cmdType == "message":
            print topicInfo

        return "", "", 0

    def printAllTopicInfo(self):
        ret, response, errMsg = self.adminDelegate.getAllTopicInfo()
        if self.fromApi:
            return ret, response, errMsg
        if not ret:
            print "Get topic info failed!"
            print errMsg
            return "", "", 1

        print "Get all topic info success!"

        allTopicInfo = response.allTopicInfo
        for topicInfo in allTopicInfo:
            topicInfoAnalyzer = status_analyzer.TopicInfoAnalyzer()
            partCount = topicInfoAnalyzer.getPartitionCount(topicInfo)
            topicName = topicInfo.name
            if self.groupName and topicInfo.topicGroup != self.groupName:
                continue
            topicStatus = swift_util.SwiftUtil.protoEnumToString(topicInfo, "status")[13:]
            print("%s.TopicStatus[%s].PartitionStatus(waiting/starting/running/stopping/recovering): "
                  "%d/%d/%d/%d/%d ") % (topicName, topicStatus,
                                        partCount.partWaiting + partCount.partNone,
                                        partCount.partStarting, partCount.partRunning,
                                        partCount.partStopping, partCount.partRecovering)
        return "", "", 0

    def run(self):
        if self.topicName is not None:
            return self.printTopicInfo()
        else:
            return self.printAllTopicInfo()


class TopicSimpleInfosCmd(base_cmd.BaseCmd):
    '''
    swift {gtsi|gettopicsimpleinfo}
       {-z zookeeper_address   | --zookeeper=zookeeper_address}

    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address

Example:
    swift gtsi -z zfs://10.250.12.23:1234/root
    '''

    def addOptions(self):
        super(TopicSimpleInfosCmd, self).addOptions()

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TopicSimpleInfosCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        return True, ''

    def initMember(self, options):
        super(TopicSimpleInfosCmd, self).initMember(options)

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errMsg = self.adminDelegate.getAllTopicSimpleInfo()
        if self.fromApi:
            return ret, response, errMsg
        print ret, response, errMsg


class TopicDeleteByTimeCmd(transport_cmd_base.TransportCmdBase):
    '''
    swift {dtt|deletetopicsbytime}
       {-z zookeeper_address     | --zookeeper=zookeeper_address }
       {-i interval              | --interval=interval }
       {-e exclude_topics,       | --exclude=exclude_topics }
       {-f exclude_topics_file,  | --exclude_file=exclude_topics_file }
       {-p prefix_name           | --prefix_name=prefix_name }
       {-t topic_names           | --topic_names=topic_names }

    options:
       -z zookeeper_address,    --zookeeper=zookeeper_address       : required, zookeeper root address
       -i time_interval,        --interval=time_interval            : required, delete topics if the last message arrived interval hours before, (unit:hours)
       -e exclude_topics,       --exclude=exclude_topics            : option, don't delete exclude topics
       -f exclude_topics_file,  --exclude_file=exclude_topics_file  : option, don't delete exclude topics in file,one topic per line
       -p prefix_name           --prefix_name=prefix_name           : option, delete topic prefix with specified
       -t topic_names           --topic_names=topic_names           : option, delete topic with specified topic names
Example:
    swift dtt -z zfs://10.250.12.23:1234/root -i 48
    swift dtt -z zfs://10.250.12.23:1234/root -i 48 -e topic_a,topic_b
    swift dtt -z zfs://10.250.12.23:1234/root -i 48 -f a.txt
    swift dtt -z zfs://10.250.12.23:1234/root -i 1 -p model_
    swift dtt -z zfs://10.250.12.23:1234/root -i 1 -t topic_name_file

    '''

    def addOptions(self):
        super(TopicDeleteByTimeCmd, self).addOptions()
        self.parser.add_option('-i', '--interval', type=int, action='store', dest='interval')
        self.parser.add_option('-e', '--exclude', action='store', dest='excludeTopics')
        self.parser.add_option('-f', '--exclude_file', action='store', dest='excludeFile')
        self.parser.add_option('-p', '--prefix_name', action='store', dest='prefixName')
        self.parser.add_option('-t', '--topic_names', action='store', dest='topicNames')

    def checkOptionsValidity(self, options):
        if not super(TopicDeleteByTimeCmd, self).checkOptionsValidity(options):
            return False

        if options.interval is None:
            print "ERROR: interval must be specified!"
            return False

        return True

    def initMember(self, options):
        super(TopicDeleteByTimeCmd, self).initMember(options)
        self.interval = options.interval
        self.prefixName = options.prefixName
        self.excludeTopics = []
        if options.excludeTopics is not None:
            self.excludeTopics = options.excludeTopics.split(",")
        if options.excludeFile is not None:
            f = open(options.excludeFile, 'r')
            for line in f.readlines():
                line = line.strip('\n')
                self.excludeTopics.append(line)
            f.close()
        print "exclude topics:", self.excludeTopics
        self.topicNames = []
        if options.topicNames is not None:
            f = open(options.topicNames, 'r')
            for line in f.readlines():
                line = line.strip('\n')
                self.topicNames.append(line)
            f.close()
        print "topics:", self.topicNames

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        self.brokerDelegate = swift_broker_delegate.SwiftBrokerDelegate(self.options.username, self.options.passwd)
        return True

    def run(self):
        delTopicNames = self.deleteTopics()
        print delTopicNames
        if (len(delTopicNames) == 0):
            print "no topic need to delete."
        return "", "", 0

    def delTopic(self, topicName):
        timeFormat = '%Y-%m-%d %X'
        ret, reponse, errorMsg = self.adminDelegate.deleteTopic(topicName)
        if not ret:
            print "[%s] Delete topic %s failed!" % (time.strftime(timeFormat, time.localtime()), topicName)
            print errorMsg
        else:
            print "[%s] Delete topic %s success!" % (time.strftime(timeFormat, time.localtime()), topicName)

    def deleteTopics(self):
        delList = []
        ret, response, errMsg = self.adminDelegate.getAllTopicInfo()
        if not ret:
            print "get all topic info failed [%s]" % errMsg
            return delList
        allTopicInfo = response.allTopicInfo
        for topicInfo in allTopicInfo:
            topicInfoAnalyzer = status_analyzer.TopicInfoAnalyzer()
            partCount = topicInfoAnalyzer.getPartitionCount(topicInfo)
            topicName = topicInfo.name
            if len(topicInfo.partitionInfos) == partCount.partRunning:
                if self.needDelete(topicInfo) and topicName not in self.excludeTopics:
                    delList.append(topicName)
                    self.delTopic(topicName)
        return delList

    def needDelete(self, topicInfo):
        topicName = topicInfo.name
        if self.prefixName is not None and not topicName.startswith(self.prefixName):
            return False
        if len(self.topicNames) > 0 and topicName not in self.topicNames:
            return False
        maxTime = 0
        allPartNoData = True
        curTime = int(time.time())
        for partInfo in topicInfo.partitionInfos:
            brokerAddr = "tcp:%s" % partInfo.brokerAddress
            ret, data = self.brokerDelegate.getMaxMessageId(brokerAddr, topicName, partInfo.id)
            if not ret:
                if data.find('ERROR_BROKER_NO_DATA') != -1:
                    allPartNoData = allPartNoData and True
                continue
            response = data
            if response.HasField(swift_common_define.PROTO_TIME_STAMP):
                if response.timestamp > maxTime:
                    maxTime = response.timestamp
        if maxTime != 0:
            maxTime = maxTime / 1000 / 1000
            print "has data, topic name:%s, not active time: %f(hour)" % (topicName, float(curTime - maxTime) / 3600)
            if float(curTime - maxTime) / 3600 > float(self.interval):
                return True
            else:
                return False
        elif allPartNoData:
            createTime = self.getTopicCreateTime(topicInfo)
            print "no data, topic name:%s, not active time: %f(hour)" % (topicName, float(curTime - createTime) / 3600)
            if float(curTime - createTime) / 3600 > float(self.interval):
                return True
            else:
                return False
        else:
            return False

    def getTopicCreateTime(self, topicInfo):
        if topicInfo.HasField("createTime"):
            return int(topicInfo.createTime) / 1000 / 1000
        else:
            path = self.zfsAddress + '/topics/' + topicInfo.topicName
            ret = self.fileUtil.getMeta(path)
            prefixStr = 'node last modify time: '
            pos = ret.find(prefixStr)
            if (pos != -1):
                timeStr = ret[pos + len(prefixStr):].strip()
                createTime = time.mktime(time.strptime(timeStr, '%Y-%m-%d %H:%M:%S'))
                return int(createTime)
            else:
                return -1


class TopicDataDeleteCmd(base_cmd.BaseCmd):
    '''
    swift {dtd|deletetopicdata}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }
       {-d dfs_root            | --dfs=dfs_root }
       {-i interval            | --interval=interval }
       {-e exclude             | --exclude=exclude_topics }
       {-t topic_name          | --topic_name=topic_name }
       {-f exclude_file        | --exclude_file=exclude_file }

    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address
       -d dfs_root,           --dfs=dfs_root                  : required, dfs root address
       -i time_interval,      --interval=time_interval        : required, delete topic data if the topic is not running and the last message is arrived interval time ago. (unit:hours)
       -e exclude_topics,     --exclude=exclude_topics        : option, don't delete the specified topic data
       -t topic_name,         --topic=topic_name              : option, only delete the specified topic data
       -f exclude_file,       --exclude_file=exclude_file     : option, don't delete the specified topic data in file

Example:
    swift dtd -z zfs://10.250.12.23:1234/root -i 72 -d hdfs://xxxx/path
    swift dtd -z zfs://10.250.12.23:1234/root -i 72 -d hdfs://xxxx/path -f abc.txt
    swift dtd -z zfs://10.250.12.23:1234/root -i 72 -d hdfs://xxxx/path -e topic_a,topic_b
    swift dtd -z zfs://10.250.12.23:1234/root -i 72 -t topic_a  -d hdfs://xxxx/path

    '''

    def addOptions(self):
        super(TopicDataDeleteCmd, self).addOptions()
        self.parser.add_option('-d', '--dfs', action='store', dest='dfsRoot')
        self.parser.add_option('-i', '--interval', type='int', action='store', dest='interval')
        self.parser.add_option('-e', '--exclude', action='store', dest='excludeTopics')
        self.parser.add_option('-t', '--topic_name', action='store', dest='topicName')
        self.parser.add_option('-f', '--exclude_file', action='store', dest='excludeFile')

    def checkOptionsValidity(self, options):
        if not super(TopicDataDeleteCmd, self).checkOptionsValidity(options):
            return False
        if options.interval is None:
            print "ERROR: interval must be specified!"
            return False
        if options.dfsRoot is None:
            print "ERROR: dfsRoot must be specified!"
            return False
        return True

    def initMember(self, options):
        super(TopicDataDeleteCmd, self).initMember(options)
        self.interval = options.interval
        self.excludeTopics = []
        if options.excludeTopics is not None:
            self.excludeTopics = options.excludeTopics.split(",")
        if options.excludeFile is not None:
            f = open(options.excludeFile, 'r')
            for line in f.readlines():
                line = line.strip('\n')
                self.excludeTopics.append(line)
            f.close()
        print "exclude topics:", self.excludeTopics
        self.specifiedTopic = options.topicName
        self.dfsRoot = options.dfsRoot

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        runningTopicName = self.getAllRunningTopicNames()
        print runningTopicName
        if self.specifiedTopic is not None and self.specifiedTopic in runningTopicName:
            print "the specified topic [%s] still running, can't delete the data." % self.specifiedTopic
            return "", "", 1
        allTopicName = self.getCandidataTopicNames()
        print allTopicName
        delTopicName = []
        for topicName in allTopicName:
            if topicName not in runningTopicName and topicName not in self.excludeTopics:
                if self.specifiedTopic is not None:
                    if topicName == self.specifiedTopic:
                        delTopicName.append(self.specifiedTopic)
                else:
                    delTopicName.append(topicName)
        self.delTopicData(delTopicName)
        return "", "", 0

    def getAllRunningTopicNames(self):
        topicList = []
        ret, response, errMsg = self.adminDelegate.getAllTopicInfo()
        if not ret:
            print "get all topic info failed [%s]." % errMsg
            return topicList
        allTopicInfo = response.allTopicInfo
        for topicInfo in allTopicInfo:
            topicList.append(topicInfo.name)
        return topicList

    def getCandidataTopicNames(self):
        topicList = []
        self.fileUtil.addHadoopHome(self.toolsConfig.getHadoopHome())
        print "get topic info in ", self.dfsRoot
        dirs = self.fileUtil.listDir(self.dfsRoot)
        print "all topic count [%d]" % len(dirs)
        curTime = int(time.time())
        for topicName in dirs:
            maxTime = self.getTopicLastModifyTime(self.fileUtil, self.dfsRoot + "/" + topicName)
            print "topic name:%s, not active time: %f(hour)" % (topicName, float(curTime - maxTime) / 3600)
            if float(curTime - maxTime) / 3600 > float(self.interval):
                topicList.append(topicName)
        return topicList

    def getTopicLastModifyTime(self, fileUtil, topicDataPath):
        modifyTime = -1
        partitions = fileUtil.listDir(topicDataPath)
        if len(partitions) == 0:
            return modifyTime
        for partition in partitions:
            files = fileUtil.listDir(topicDataPath + "/" + partition)
            if len(files) == 0:
                continue
            else:
                files.sort()
                metaInfo = fileUtil.getMeta(topicDataPath + "/" + partition + "/" + files[len(files) - 1])
                prefix = "file last modify time: "
                pos = metaInfo.find(prefix)
                if pos != -1:
                    timeStr = metaInfo[pos + len(prefix):]
                    timeStr = timeStr.strip()
                    createTime = int(time.mktime(time.strptime(timeStr, '%Y-%m-%d %H:%M:%S')))
                    if createTime > modifyTime:
                        modifyTime = createTime
        return modifyTime

    def delTopicData(self, topicNames):
        for topic in topicNames:
            print "deleting topic data [%s]" % topic
            ret = self.fileUtil.remove(self.dfsRoot + "/" + topic)
            if not ret:
                print "delete topic data [%s] failed!" % topic


class ExportTopicsCmd(base_cmd.BaseCmd):
    '''
    swift {et|exporttopics}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }
       {-f export_file_name    | --file=export_file_name }
       {-g group_name          | --group=group_name }
       {-p prefix              | --prefix=prefix_name }
       {-t topic_names         | --topic=topic_names }

    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address
       -f export_file_name,   --file=export_file_name         : required, export file name
       -g group_name,         --group=group_name              : optional, export group name, default export all topic
       -m migrateDfs,         --migrate=migrate_dfs           : optional, migrate dfs, add new hdfs, add current dfs into extendDfs root
       -p prefix,             --prefix = prefix               : optional, export topic info with prefix
       -i topicName,          --topic = topic_names           : optional, export topic info with topic_name file
Example:
    swift et -z zfs://10.250.12.23:1234/root -f a.json
    swift et -z zfs://10.250.12.23:1234/root -f a.json -g swift
    swift et -z zfs://10.250.12.23:1234/root -f a.json -p prefix
    swift et -z zfs://10.250.12.23:1234/root -f a.json -i topicNameFile
    '''

    def addOptions(self):
        super(ExportTopicsCmd, self).addOptions()
        self.parser.add_option('-f', '--file', action='store', dest='fileName')
        self.parser.add_option('-g', '--group', action='store', dest='groupName')
        self.parser.add_option('-m', '--migrate', action='store', dest='migrateDfs')
        self.parser.add_option('-p', '--prefix', action='store', dest='prefix')
        self.parser.add_option('-i', '--topic', action='store', dest='topicNames')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(ExportTopicsCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if options.fileName is None:
            errMsg = "ERROR: file name must be specified!"
            print errMsg
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(ExportTopicsCmd, self).initMember(options)
        self.fileName = options.fileName
        self.localFileUtil = local_file_util.LocalFileUtil()
        self.groupName = options.groupName
        self.migrateDfs = options.migrateDfs
        self.prefix = options.prefix
        self.topicNames = []
        if options.topicNames:
            f = open(options.topicNames, 'r')
            for line in f.readlines():
                line = line.strip('\n')
                self.topicNames.append(line)
            f.close()

    def getDfsRoot(self):
        versionFile = self.zfsAddress + "/config/version"
        localConfigFile = "./.swift.conf"
        configFile = self.zfsAddress + "/config/swift.conf"
        if self.fileUtil.exists(versionFile):
            configFile = self.getLatestConfig(self.zfsAddress + "/config") + "/swift.conf"
        self.fileUtil.copy(configFile, localConfigFile, True)
        configLines = self.localFileUtil.readLines(localConfigFile)
        for line in configLines:
            line = line.strip()
            if line.startswith("data_root_path"):
                strVec = line.split("=")
                if len(strVec) == 2:
                    return strVec[1].strip()
        return ""

    def hasDfsRoot(self, metaStr):
        metaVec = metaStr.split('\n')
        for metaMeg in metaVec:
            metaMeg = metaMeg.strip()
            if metaMeg.startswith("dfsRoot"):
                dfsVec = metaMeg.split("Root: ")
                if len(dfsVec) == 2 and len(dfsVec[1].strip()) > 2:
                    return True
        return False

    def hasTopicGroup(self, metaStr):
        metaVec = metaStr.split('\n')
        for metaMeg in metaVec:
            metaMeg = metaMeg.strip()
            if metaMeg.startswith("topicGroup"):
                dfsVec = metaMeg.split("opicGroup: ")
                if len(dfsVec) == 2 and len(dfsVec[1].strip()) > 2:
                    return True
        return False

    def generateMap(self, topicMetas):
        retMap = {}
        for meta in topicMetas.topicMetas:
            tmpMap = {}
            if self.groupName is not None:
                if self.hasTopicGroup(str(meta)):
                    if meta.topicGroup != self.groupName:
                        continue
                else:
                    if self.groupName != 'default':
                        continue
            if self.prefix is not None and not meta.topicName.startswith(self.prefix):
                continue
            if len(self.topicNames) > 0 and meta.topicName not in self.topicNames:
                continue
            metaVec = str(meta).split('\n')
            for metaMsg in metaVec:
                if metaMsg == '':
                    continue
                metaMsg = metaMsg.strip()
                pos = metaMsg.find(':')
                metaMsgVec = []
                metaMsgVec.append(metaMsg[0:pos])
                metaMsgVec.append(metaMsg[pos + 1:len(metaMsg)])
                if metaMsgVec[1] is not None:
                    metaMsgVec[1] = metaMsgVec[1].strip()
                    metaMsgVec[0] = metaMsgVec[0].strip()
                    if metaMsgVec[1].startswith('"') and metaMsgVec[1].endswith('"'):
                        metaMsgVec[1] = metaMsgVec[1].rstrip('"').lstrip('"')
                    if tmpMap.has_key(metaMsgVec[0]):
                        tmpMap[metaMsgVec[0]] += ',' + metaMsgVec[1]
                    else:
                        tmpMap[metaMsgVec[0]] = metaMsgVec[1]
                else:
                    stripKey = metaMsgVec[0].strip()
                    if tmpMap.has_key(stripKey):
                        tmpMap[stripKey] += ','
                    else:
                        tmpMap[stripKey] = ''
            if self.migrateDfs:
                self.updateMigrateDfs(tmpMap)
            retMap[meta.topicName] = tmpMap
        return retMap

    def updateMigrateDfs(self, topicInfo):
        dfsRoot = topicInfo["dfsRoot"]
        if topicInfo.has_key("extendDfsRoot"):
            extendDfsRoot = topicInfo["extendDfsRoot"]
            extendDfsList = extendDfsRoot.split(',')
            if dfsRoot not in extendDfsList:
                extendDfsList.append(dfsRoot)
            if self.migrateDfs in extendDfsList:
                extendDfsList.remove(self.migrateDfs)
            topicInfo["extendDfsRoot"] = ",".join(extendDfsList)
        else:
            topicInfo["extendDfsRoot"] = dfsRoot
        topicInfo["dfsRoot"] = self.migrateDfs

    def run(self):
        metaFile = self.zfsAddress + "/topic_meta"
        localFile = "./.topic_meta"
        self.fileUtil.copy(metaFile, localFile, True)
        metaStr = self.localFileUtil.cat(localFile)
        deCompStr = zlib.decompress(metaStr)
        topicMetas = TopicMetas()
        topicMetas.ParseFromString(deCompStr)
        if self.fromApi:
            return 0, topicMetas, 0
        dfsRoot = self.getDfsRoot()
        for meta in topicMetas.topicMetas:
            if not self.hasDfsRoot(str(meta)):
                meta.dfsRoot = dfsRoot
        retMap = self.generateMap(topicMetas)
        print "export topic count ", len(retMap)
        retJson = json.write(retMap, format=True)
        self.localFileUtil.write(retJson, self.fileName, 'w')
        return "", "", 0


class TopicMetaCmd(base_cmd.BaseCmd):
    '''
    swift {gtm|gettopicmetas}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }

    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address
Example:
    swift gtm -z zfs://10.250.12.23:1234/root
    '''

    def addOptions(self):
        super(TopicMetaCmd, self).addOptions()

    def initMember(self, options):
        super(TopicMetaCmd, self).initMember(options)
        self.localFileUtil = local_file_util.LocalFileUtil()

    def run(self):
        metaFile = self.zfsAddress + "/topic_meta"
        localFile = "./.topic_meta"
        self.fileUtil.copy(metaFile, localFile, True)
        metaStr = self.localFileUtil.cat(localFile)
        if 0 == len(metaStr):
            return "", "", 1
        deCompStr = zlib.decompress(metaStr)
        topicMetas = TopicMetas()
        topicMetas.ParseFromString(deCompStr)
        if self.fromApi:
            return "", topicMetas, 0
        print topicMetas
        return "", "", 0


class ImportTopicsCmd(base_cmd.BaseCmd):
    '''
    swift {it|importtopics}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }
       {-f local_file_name     | --file=local_file_name }
       {-w time_out            | --timeout=time_out}
       {-d delete_exist        | --delete_exist=delete_exist}
       {-c continue            | --continue=continue}
       {-g group               | --group=group}

    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address
       -f local_file_name,    --file=local_file_name          : required, local file name
       -w time_out,           --timeout=time_out              : optional, timeout for one topic
       -d delete_exist        --delete_exist=delete_exist     : optional, delete exist topic ,default is false
       -c continue_exist      --continue=continue             : optional, continue if add topic has error ,default is false
       -g group               --group=group                   : optional, add in specified group name
Example:
    swift it -z zfs://10.250.12.23:1234/root -f a.json -w 30
    swift it -z zfs://10.250.12.23:1234/root -f a.json -d -c
    swift it -z zfs://10.250.12.23:1234/root -f a.json -g igraph
    '''

    def addOptions(self):
        super(ImportTopicsCmd, self).addOptions()
        self.parser.add_option('-f', '--file', action='store', dest='fileName')
        self.parser.add_option('-w', '--timeout', type='int',
                               action='store', dest='timeout')
        self.parser.add_option('-d', '--delete_exist', action='store_true', dest='deleteExist',
                               default=False)
        self.parser.add_option('-c', '--continue', action='store_true', dest='continueAdd',
                               default=False)
        self.parser.add_option('-g', '--group', action='store', dest='group')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(ImportTopicsCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if options.fileName is None:
            errMsg = "ERROR: file name must be specified!"
            print errMsg
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(ImportTopicsCmd, self).initMember(options)
        self.fileName = options.fileName
        self.timeout = options.timeout
        self.deleteExist = options.deleteExist
        self.continueAdd = options.continueAdd
        self.localFileUtil = local_file_util.LocalFileUtil()
        self.group = options.group
        self.failTopicFile = './fail_topic_list_for_add_' + str(time.time())

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        content = self.localFileUtil.cat(self.fileName)
        topicMap = json.read(content)
        topicName = None
        failTopicList = {}
        hasError = False
        timeFormat = '%Y-%m-%d %X'
        for topic, topicDescription in topicMap.items():
            partitionCount = None
            resource = None
            partitionLimit = None
            topicMode = None
            needFieldFilter = None
            obsoleteFileTimeInterval = None
            reservedFileCount = None
            deleteTopicData = None
            partitionMinBufferSize = None
            partitionMaxBufferSize = None
            maxWaitTimeForSecurityCommit = None
            maxDataSizeForSecurityCommit = None
            compressMsg = None
            compressThres = None
            dfsRoot = None
            topicGroup = None
            extendDfsRoot = None
            topicExpiredTime = None
            rangeCnt = None
            owners = None
            needSchema = None
            schemaVersions = None
            sealed = None
            topicType = None
            physicTopicLst = None
            enableTTLDel = None
            if topicDescription.has_key("topicName"):
                topicName = topicDescription["topicName"]
            if topicDescription.has_key("partitionCount"):
                partitionCount = int(topicDescription["partitionCount"])
            if topicDescription.has_key("resource"):
                resource = int(topicDescription["resource"])
            if topicDescription.has_key("partitionLimit"):
                partitionLimit = int(topicDescription["partitionLimit"])
            if topicDescription.has_key("topicMode"):
                if topicDescription["topicMode"] == 'TOPIC_MODE_NORMAL':
                    topicMode = TOPIC_MODE_NORMAL
                elif topicDescription["topicMode"] == 'TOPIC_MODE_SECURITY':
                    topicMode = TOPIC_MODE_SECURITY
                elif topicDescription["topicMode"] == 'TOPIC_MODE_MEMORY_PREFER':
                    topicMode = TOPIC_MODE_MEMORY_PREFER
                elif topicDescription["topicMode"] == 'TOPIC_MODE_MEMORY_ONLY':
                    topicMode = TOPIC_MODE_MEMORY_ONLY
            if topicDescription.has_key("needFieldFilter"):
                needFieldFilter = topicDescription["needFieldFilter"] == 'true'
            if topicDescription.has_key("obsoleteFileTimeInterval"):
                obsoleteFileTimeInterval = int(topicDescription["obsoleteFileTimeInterval"])
            if topicDescription.has_key("reservedFileCount"):
                reservedFileCount = int(topicDescription["reservedFileCount"])
            if topicDescription.has_key("deleteTopicData"):
                deleteTopicData = topicDescription["deleteTopicData"] == 'true'
            if topicDescription.has_key("partitionMinBufferSize"):
                partitionMinBufferSize = int(topicDescription["partitionMinBufferSize"])
            if topicDescription.has_key("partitionMaxBufferSize"):
                partitionMaxBufferSize = int(topicDescription["partitionMaxBufferSize"])
            if topicDescription.has_key("maxWaitTimeForSecurityCommit"):
                maxWaitTimeForSecurityCommit = int(topicDescription["maxWaitTimeForSecurityCommit"])
            if topicDescription.has_key("maxDataSizeForSecurityCommit"):
                maxDataSizeForSecurityCommit = int(topicDescription["maxDataSizeForSecurityCommit"])
            if topicDescription.has_key("compressMsg"):
                compressMsg = topicDescription["compressMsg"] == 'true'
            if topicDescription.has_key("compressThres"):
                compressThres = int(topicDescription["compressThres"])
            if topicDescription.has_key("dfsRoot"):
                dfsRoot = topicDescription["dfsRoot"]
            if topicDescription.has_key("extendDfsRoot"):
                extendDfsRoot = topicDescription["extendDfsRoot"]
            if topicDescription.has_key("topicGroup"):
                topicGroup = topicDescription["topicGroup"]
            if topicDescription.has_key("topicExpiredTime"):
                topicExpiredTime = int(topicDescription["topicExpiredTime"])
            if topicDescription.has_key("rangeCount"):
                rangeCnt = int(topicDescription["rangeCount"])
            if topicDescription.has_key("owners"):
                owners = topicDescription["owners"]
            if topicDescription.has_key("needSchema"):
                needSchema = topicDescription["needSchema"] == 'true'
                if topicDescription.has_key("schemaVersions"):
                    schemaVersions = topicDescription["schemaVersions"]
            if topicDescription.has_key("sealed"):
                sealed = topicDescription["sealed"] == 'true'
            if topicDescription.has_key("topicType"):
                if topicDescription.has_key("topicType") == "TOPIC_TYPE_NORMAL":
                    topicType = TOPIC_TYPE_NORMAL
                elif topicDescription.has_key("topicType") == "TOPIC_TYPE_PHYSIC":
                    topicType = TOPIC_TYPE_PHYSIC
                elif topicDescription.has_key("topicType") == "TOPIC_TYPE_LOGIC":
                    topicType = TOPIC_TYPE_LOGIC
                elif topicDescription.has_key("topicType") == "TOPIC_TYPE_LOGIC_PHYSIC":
                    topicType = TOPIC_TYPE_LOGIC_PHYSIC
            if topicDescription.has_key("physicTopicLst"):
                physicTopicLst = topicDescription["physicTopicLst"]
            if topicDescription.has_key("enableTTLDel"):
                enableTTLDel = topicDescription["enableTTLDel"] == 'true'
            if self.group is not None:
                topicGroup = self.group
            if hasError and not self.continueAdd:
                failTopicList[topicName] = topicDescription
                continue
            if self.deleteExist:
                ret, reponse, errorMsg = self.adminDelegate.deleteTopic(topicName)
                if not ret:
                    if errorMsg.find('ERROR_ADMIN_TOPIC_NOT_EXISTED') == -1:
                        print "[%s] Delete topic %s failed! error msg [%s]" % (time.strftime(timeFormat, time.localtime()), topicName, errorMsg)
                        hasError = True
                        continue
                else:
                    print "[%s] Delete topic %s success!" % (time.strftime(timeFormat, time.localtime()), topicName)
                    time.sleep(2)

            ret, response, errorMsg = self.adminDelegate.createTopic(
                topicName=topicName, partCnt=partitionCount, rangeCnt=rangeCnt,
                partMinBufSize=partitionMinBufferSize, partMaxBufSize=partitionMaxBufferSize,
                partResource=resource, partLimit=partitionLimit,
                topicMode=topicMode, needFieldFilter=needFieldFilter,
                obsoleteFileTimeInterval=obsoleteFileTimeInterval,
                reservedFileCount=reservedFileCount,
                deleteTopicData=deleteTopicData,
                securityCommitTime=maxWaitTimeForSecurityCommit,
                securityCommitData=maxDataSizeForSecurityCommit,
                compressMsg=compressMsg,
                compressThres=compressThres,
                dfsRoot=dfsRoot, topicGroup=topicGroup, extendDfsRoot=extendDfsRoot,
                expiredTime=topicExpiredTime, owners=owners, needSchema=needSchema,
                schemaVersions=schemaVersions, sealed=sealed, topicType=topicType,
                physicTopicLst=physicTopicLst, enableTTLDel=enableTTLDel)
            if not ret:
                print "[%s] Add topic [%s] Failed, error msg [%s]" % (time.strftime(timeFormat, time.localtime()), topicName, errorMsg)
                failTopicList[topicName] = topicDescription
                hasError = True
                continue

            if self.timeout is not None:
                print "wait topic ready."
                if not self._waitTopicReady(self.timeout, topicName):
                    print "wait topic %s ready failed" % topicName
                    failTopicList[topicName] = topicDescription
                    hasError = True
                    continue
            print "[%s] Add topic [%s] success!" % (time.strftime(timeFormat, time.localtime()), topicName)
        if len(failTopicList) > 0:
            self.addFailTopic(failTopicList)
        return "", "", 0

    def addFailTopic(self, failTopicList):
        failTopic = json.write(failTopicList, format=True)
        return self.localFileUtil.write(failTopic, self.failTopicFile, 'w')

    def _waitTopicReady(self, timeout, topicName):
        while timeout > 0:
            if self._isTopicReady(topicName):
                print "topic [%s] is ready" % topicName
                return "", "", 0
            timeout -= 0.1
            time.sleep(0.1)
        return "", "wait topic ready timeout.", -1

    def _isTopicReady(self, topicName):
        ret, response, errMsg = self.adminDelegate.getTopicInfo(topicName)
        if ret:
            if response.HasField(swift_common_define.PROTO_TOPIC_INFO):
                topicInfo = response.topicInfo
                for partitionInfo in topicInfo.partitionInfos:
                    if partitionInfo.status != PARTITION_STATUS_RUNNING:
                        print "ERROR: partition info status: %s, response: %s" % \
                            (str(partitionInfo), str(response))
                        return False
            else:
                print "ERROR: response does not have topicInfo field! response: %s" % str(response)
                return False
        else:
            print "get topic info failed: %s, errorMsg: %s" % (topicName, errMsg)
            return False
        return True


class UpdateTopicsCmd(base_cmd.BaseCmd):
    '''
    swift {ut|updatetopics}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }
       {-f local_file_name     | --file=local_file_name }
       {-w time_out            | --timeout=time_out}

    options:
       -z zookeeper_address,  --zookeeper=zookeeper_address   : required, zookeeper root address
       -f local_file_name,    --file=local_file_name          : required, local file name
       -w time_out,           --timeout=time_out              : optional, timeout for one topic

Example:
    swift ut -z zfs://10.250.12.23:1234/root -f a.json -w 30
    '''

    def addOptions(self):
        super(UpdateTopicsCmd, self).addOptions()
        self.parser.add_option('-f', '--file', action='store', dest='fileName')
        self.parser.add_option('-w', '--timeout', type='int',
                               action='store', dest='timeout')

    def checkOptionsValidity(self, options):
        if not super(UpdateTopicsCmd, self).checkOptionsValidity(options):
            return False
        if options.fileName is None:
            print "ERROR: file name must be specified!"
            return False
        return True

    def initMember(self, options):
        super(UpdateTopicsCmd, self).initMember(options)
        self.fileName = options.fileName
        self.timeout = options.timeout
        self.localFileUtil = local_file_util.LocalFileUtil()
        self.failTopicFile = './fail_topic_list_for_add'

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        content = self.localFileUtil.cat(self.fileName)
        topicMap = json.read(content)
        topics = sorted(topicMap.items(), key=lambda x: x[1])
        for (topic, topicDescription) in topics:
            if topic != topicDescription["topicName"]:
                print "error, topic name not equal [%s %s]" % (topic, topicDescription["topicName"])
                return "", "", 0
            ret, response, errorMsg = self.adminDelegate.deleteTopic(topic)
            if not ret:
                print "delete topic [%s] error, %s" % (topic, errorMsg)
            else:
                print "delete topic [%s] success." % topic
            time.sleep(2)  # wait partition unload
            if not self.addOneTopic(topicDescription):
                print "not all topic updated, retry!"
                return "", "", 0
        return "", "", 0

    def addOneTopic(self, topicDescription):
        partitionCount = None
        resource = None
        partitionLimit = None
        topicMode = None
        needFieldFilter = None
        obsoleteFileTimeInterval = None
        reservedFileCount = None
        deleteTopicData = None
        partitionMinBufferSize = None
        partitionMaxBufferSize = None
        maxWaitTimeForSecurityCommit = None
        maxDataSizeForSecurityCommit = None
        compressMsg = None
        compressThres = None
        dfsRoot = None
        topicGroup = None
        extendDfsRoot = None
        if topicDescription.has_key("topicName"):
            topicName = topicDescription["topicName"]
            if topicDescription.has_key("partitionCount"):
                partitionCount = int(topicDescription["partitionCount"])
            if topicDescription.has_key("resource"):
                resource = int(topicDescription["resource"])
            if topicDescription.has_key("partitionLimit"):
                partitionLimit = int(topicDescription["partitionLimit"])
            if topicDescription.has_key("topicMode"):
                if topicDescription["topicMode"] == 'TOPIC_MODE_NORMAL':
                    topicMode = TOPIC_MODE_NORMAL
                elif topicDescription["topicMode"] == 'TOPIC_MODE_SECURITY':
                    topicMode = TOPIC_MODE_SECURITY
            if topicDescription.has_key("needFieldFilter"):
                needFieldFilter = topicDescription["needFieldFilter"] == 'true'
            if topicDescription.has_key("obsoleteFileTimeInterval"):
                obsoleteFileTimeInterval = int(topicDescription["obsoleteFileTimeInterval"])
            if topicDescription.has_key("reservedFileCount"):
                reservedFileCount = int(topicDescription["reservedFileCount"])
            if topicDescription.has_key("deleteTopicData"):
                deleteTopicData = topicDescription["deleteTopicData"] == 'true'
            if topicDescription.has_key("partitionMinBufferSize"):
                partitionMinBufferSize = int(topicDescription["partitionMinBufferSize"])
            if topicDescription.has_key("partitionMaxBufferSize"):
                partitionMaxBufferSize = int(topicDescription["partitionMaxBufferSize"])
            if topicDescription.has_key("maxWaitTimeForSecurityCommit"):
                maxWaitTimeForSecurityCommit = int(topicDescription["maxWaitTimeForSecurityCommit"])
            if topicDescription.has_key("maxDataSizeForSecurityCommit"):
                maxDataSizeForSecurityCommit = int(topicDescription["maxDataSizeForSecurityCommit"])
            if topicDescription.has_key("compressMsg"):
                compressMsg = topicDescription["compressMsg"] == 'true'
            if topicDescription.has_key("compressThres"):
                compressThres = int(topicDescription["compressThres"])
            if topicDescription.has_key("dfsRoot"):
                dfsRoot = topicDescription["dfsRoot"]
            if topicDescription.has_key("extendDfsRoot"):
                extendDfsRoot = topicDescription["extendDfsRoot"]
            if topicDescription.has_key("topicGroup"):
                topicGroup = topicDescription["topicGroup"]

        ret, response, errorMsg = self.adminDelegate.createTopic(
            topicName=topicName, partCnt=partitionCount, rangeCnt=self.rangeCnt,
            partMinBufSize=partitionMinBufferSize, partMaxBufSize=partitionMaxBufferSize,
            partResource=resource, partLimit=partitionLimit,
            topicMode=topicMode, needFieldFilter=needFieldFilter,
            obsoleteFileTimeInterval=obsoleteFileTimeInterval,
            reservedFileCount=reservedFileCount,
            deleteTopicData=deleteTopicData,
            securityCommitTime=maxWaitTimeForSecurityCommit,
            securityCommitData=maxDataSizeForSecurityCommit,
            compressMsg=compressMsg,
            compressThres=compressThres,
            dfsRoot=dfsRoot, topicGroup=topicGroup, extendDfsRoot=extendDfsRoot)
        if not ret:
            print "Add topic [%s] Failed!" % topicName
            print errorMsg
            return False
        if self.timeout is not None:
            print "wait topic [%s] ready." % topicName
            if not self._waitTopicReady(self.timeout, topicName):
                print "wait topic %s ready failed" % topicName
                return False
        print "update topic %s success!" % topicName
        return True

    def _waitTopicReady(self, timeout, topicName):
        while timeout > 0:
            if self._isTopicReady(topicName):
                print "topic [%s] is ready" % topicName
                return "", "", 0
            timeout -= 0.1
            time.sleep(0.1)
        return "", "wait topic ready timeout.", -1

    def _isTopicReady(self, topicName):
        ret, response, errMsg = self.adminDelegate.getTopicInfo(topicName)
        if ret:
            if response.HasField(swift_common_define.PROTO_TOPIC_INFO):
                topicInfo = response.topicInfo
                for partitionInfo in topicInfo.partitionInfos:
                    if partitionInfo.status != PARTITION_STATUS_RUNNING:
                        print "ERROR: partition info status: %s, response: %s" % \
                            (str(partitionInfo), str(response))
                        return False
            else:
                print "ERROR: response does not have topicInfo field! response: %s" % str(response)
                return False
        else:
            print "get topic info failed: %s, errorMsg: %s" % (topicName, errMsg)
            return False
        return True


class TransferPartitionCmd(base_cmd.BaseCmd):
    '''
    swift {tp|transferpartition}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }
       {-t transfer_info       | --transfer=transfer_info }
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address     : required, zookeeper root address
       -t transfer_info,         --transfer=transfer_info          : required, topic name, eg: role_name1:0.3,role_name2:0.1
       -g group_name,            --group_name=group_name           : opitional, group name, eg: swift_mainse

Example:
    swift tp -z zfs://10.250.12.23:1234/root -t group1##broker_1_0:0.3;group2##broker_1_0:0.2
    swift tp -z zfs://10.250.12.23:1234/root -t all
    swift tp -z zfs://10.250.12.23:1234/root -t all -g swift_mainse
    '''

    def addOptions(self):
        super(TransferPartitionCmd, self).addOptions()
        self.parser.add_option('-t', '--transfer', action='store', dest='transferInfo')
        self.parser.add_option('-g', '--group_name', action='store', dest='groupName')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TransferPartitionCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if options.transferInfo is None:
            errMsg = "ERROR: transfer info must be specified!"
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(TransferPartitionCmd, self).initMember(options)
        self.groupName = options.groupName
        self.transferInfo = options.transferInfo

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.transferPartition(
            self.transferInfo, self.groupName)
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print "transfer partition failed!"
            print errorMsg
            return "", "", 1
        print "transfer partition success!"
        return "", "", 0


class ChangeSlotCmd(base_cmd.BaseCmd):
    '''
    swift {cs|changeSlot}
       {-z zookeeper_address   | --zookeeper=zookeeper_address }
       {-r role_names       | --rolenames=role_names }
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address     : required, zookeeper root address
       -r role_names,         --rolenames=role_names          : required, role name, eg: default##broker_4_0,tisplus##broker_0_0

Example:
    swift cs -z zfs://10.250.12.23:1234/root -r group1##broker_1_0,group2##broker_2_0
    '''

    def addOptions(self):
        super(ChangeSlotCmd, self).addOptions()
        self.parser.add_option('-r', '--rolenames', action='store', dest='roleNames')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(ChangeSlotCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if options.roleNames is None:
            errMsg = "ERROR: roleNames must be specified!"
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(ChangeSlotCmd, self).initMember(options)
        self.roleNames = options.roleNames

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.changeSlot(self.roleNames)
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print "change slot failed!"
            print errorMsg
            return "", "", 1
        print "change slot success!"
        return "", "", 0


class RegisterSchemaCmd(base_cmd.BaseCmd):
    '''
    swift {regscm|registerSchema}
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,            --topic_name                    : required, topic name
       -s schema,                --schema                        : required, schema json string
       -v version,               --version                       : optional, user defined int version
       -c cover,                 --cover                         : optional, to cover old versions or not if schema versions full, default is false

Example:
    swift regscm -z zfs://10.250.12.23:1234/root -t mainse -s "{\"topic\":\"mainse\",\"fields\":[{\"name\":\"nid\",\"type\":\"string\"}]}"
    '''

    def addOptions(self):
        super(RegisterSchemaCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')
        self.parser.add_option('-s', '--schema', action='store', dest='schema')
        self.parser.add_option('-v', '--version', action='store', dest='version')
        self.parser.add_option('-c', '--cover', action='store_true', dest='cover', default=False)

    def checkOptionsValidity(self, options):
        ret, errMsg = super(RegisterSchemaCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if options.topicName is None:
            errMsg = "ERROR: topic name must be specified!"
            return False, errMsg
        if options.schema is None:
            errMsg = "ERROR: schema must be specified!"
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(RegisterSchemaCmd, self).initMember(options)
        self.schema = options.schema
        self.topicName = options.topicName
        self.version = options.version
        self.cover = options.cover

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.registerSchema(self.topicName, self.schema,
                                                                    self.version, self.cover)
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print "register schema failed!"
            print errorMsg
            return "", "", 1
        print "register schema[%d] success!" % response.version
        return response.version, "", 0


class GetSchemaCmd(base_cmd.BaseCmd):
    '''
    swift {gscm|getSchemaCmd}
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,            --topic_name                    : required, topic name
       -v version,               --version                       : required, schema version

Example:
    swift gscm -z zfs://10.250.12.23:1234/root -t mainse -v 1234
    '''

    def addOptions(self):
        super(GetSchemaCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')
        self.parser.add_option('-v', '--version', action='store', dest='version')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(GetSchemaCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if options.topicName is None:
            errMsg = "ERROR: topic name must be specified!"
            return False, errMsg
        if options.version is None:
            errMsg = "ERROR: version must be specified!"
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(GetSchemaCmd, self).initMember(options)
        self.topicName = options.topicName
        self.version = options.version

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.getSchema(self.topicName, self.version)
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print "get schema failed!"
            print errorMsg
            return "", "", 1
        print response.schemaInfo
        return response.schemaInfo, "", 0


class GetTopicRWTimeCmd(base_cmd.BaseCmd):
    '''
    swift {gtrw|getopicrwtime}
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,            --topic_name                    : optional, topic name

Example:
    swift gtrw -z zfs://10.250.12.23:1234/root -t mainse
    '''

    def addOptions(self):
        super(GetTopicRWTimeCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(GetTopicRWTimeCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(GetTopicRWTimeCmd, self).initMember(options)
        self.topicName = options.topicName

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.getTopicRWTime(self.topicName)
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print errorMsg
            return "", errorMsg, 1
        print response
        return response, "", 0


class GetLastDeletedNoUseTopicCmd(base_cmd.BaseCmd):
    '''
    swift {gtldnt|getlastdeletednousetopic}
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address   : required, zookeeper root address

Example:
    swift gtldnt -z zfs://10.250.12.23:1234/root
    '''

    def addOptions(self):
        super(GetLastDeletedNoUseTopicCmd, self).addOptions()

    def checkOptionsValidity(self, options):
        ret, errMsg = super(GetLastDeletedNoUseTopicCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(GetLastDeletedNoUseTopicCmd, self).initMember(options)

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.getLastDeletedNoUseTopic()
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print errorMsg
            return "", errorMsg, 1
        else:
            print response
            return response, "", 0


class GetDeletedNoUseTopicCmd(base_cmd.BaseCmd):
    '''
    swift {gtdnt|getdeletednousetopic}
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address   : required, zookeeper root address
       -f file_name,             --file_name                     : required, deleted topic file name

Example:
    swift gtdnt -z zfs://10.250.12.23:1234/root -f 163234200
    '''

    def addOptions(self):
        super(GetDeletedNoUseTopicCmd, self).addOptions()
        self.parser.add_option('-f', '--file_name', action='store', dest='fileName')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(GetDeletedNoUseTopicCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if options.fileName is None:
            errMsg = "ERROR: file name must be specified!"
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(GetDeletedNoUseTopicCmd, self).initMember(options)
        self.fileName = options.fileName

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.getDeletedNoUseTopic(self.fileName)
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print errorMsg
            return "", errorMsg, 1
        else:
            print response
            return response, "", 0


class GetDeletedNoUseTopicFilesCmd(base_cmd.BaseCmd):
    '''
    swift {gtdntf|getdeletednousetopicfiles}
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address   : required, zookeeper root address

Example:
    swift gtdntf -z zfs://10.250.12.23:1234/root
    '''

    def addOptions(self):
        super(GetDeletedNoUseTopicFilesCmd, self).addOptions()

    def checkOptionsValidity(self, options):
        ret, errMsg = super(GetDeletedNoUseTopicFilesCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(GetDeletedNoUseTopicFilesCmd, self).initMember(options)

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.getDeletedNoUseTopicFiles()
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print errorMsg
            return "", errorMsg, 1
        else:
            print response
            return response, "", 0


class TopicAclManageCmd(base_cmd.BaseCmd):
    '''
    swift {tam|topicaclmanage}
       {-z zookeeper_address           | --zookeeper=zookeeper_address }
       {-a access_operator             | --accessop=access_operator }
       {-t topic_name                  | --topic=topic_name }
       {-u access_id                   | --accessid=access_id }
       {-k access_key                  | --accesskey=access_key }
       {-p access_priority             | --accessprior=access_priority }
       {-y access_type                 | --accesstype=access_type }
       {-s check_status                | --checkstatus=check_status }

    options:
       -z zookeeper_address,      --zookeeper=zookeeper_address         : required, zookeeper root address
       -a access_operator,        --accessop=access_operator            : required, operator to be taken, should be one of ['add', 'delete', 'clear', 'update', 'key_update', 'priority_update',  'type_update', 'status', 'list', 'listall']
       -t topic_name,             --topic=topic_name                    : required except accessop='listall', topic name to be operated
       -u access_id,              --accessid=access_id                  : required when accessop='add', '*update', 'delete', access id to be operated
       -k access_key,             --accesskey=access_key                : required when accessop='add', 'key_update', new access key to be set
       -p access_priority,        --accessprior=access_priority         : required when accessop='priority_update', new priority to be set, should be one of ['p0', 'p1',...,'p5']
       -y access_type,            --accesstype=access_type              : required when accessop='type_update', new access type to be set, should be one of ['read_only', 'read_write', 'none']
       -s check_status,           --checkstatus=check_status            : required when accessop='status', check status to be set, should be one of ['on', 'off']
    '''

    def addOptions(self):
        super(TopicAclManageCmd, self).addOptions()
        self.parser.add_option('-a', '--accessop',
                               type='choice',
                               action='store',
                               dest='accessOp',
                               choices=['add',
                                        'delete',
                                        'clear',
                                        'update',
                                        'key_update',
                                        'priority_update',
                                        'type_update',
                                        'status',
                                        'list',
                                        'listall'])
        self.parser.add_option('-t', '--topic', action='store', dest="topicName")
        self.parser.add_option('-u', '--accessid', action='store', dest='accessId')
        self.parser.add_option('-k', '--accesskey', action='store', dest='accessKey')
        self.parser.add_option('-p',
                               '--accessprior',
                               type='choice',
                               action='store',
                               dest='accessPriority',
                               choices=['p0',
                                        'p1',
                                        'p2',
                                        'p3',
                                        'p4',
                                        'p5'])
        self.parser.add_option('-y',
                               '--accesstype',
                               type='choice',
                               action='store',
                               dest='accessType',
                               choices=['read_only',
                                        'read_write',
                                        'none'])
        self.parser.add_option('-s',
                               '--check_status',
                               type='choice',
                               action='store',
                               dest='checkStatus',
                               choices=['on', 'off'])

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TopicAclManageCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.accessOp is None:
            errMsg = "ERROR: accessop must be specified"
            return False, errMsg

        if options.topicName is None and options.accessOp != 'listall':
            errMsg = "ERROR: topic must be specified when %s" % options.accessOp
            return False, errMsg

        if options.accessId is None and "update" in options.accessOp:
            errMsg = "ERROR: accessid must be specified when %s" % options.accessOp
            return False, errMsg

        if options.accessId is None and options.accessOp == 'delete':
            errMsg = "ERROR: accessid must be specified when delete"
            return False, errMsg

        if options.accessOp == 'add' and (options.accessId is None or options.accessKey is None):
            errMsg = "ERROR: accessid/accesskey must be specified when add"
            return False, errMsg

        if options.accessOp == 'key_update' and options.accessKey is None:
            errMsg = "ERROR: accesskey must be specified when key_update"
            return False, errMsg

        if options.accessOp == 'priority_update' and options.accessPriority is None:
            errMsg = "ERROR: accessprior must be specified when priority_update"
            return False, errMsg

        if options.accessOp == 'type_update' and options.accessType is None:
            errMsg = "ERROR: accesstype must be specified when type_update"
            return False, errMsg

        if options.accessOp == 'status' and options.checkStatus is None:
            errMsg = "ERROR: checkstatus must be specified when status"
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(TopicAclManageCmd, self).initMember(options)
        if options.accessOp == 'add':
            self.accessOp = ADD_TOPIC_ACCESS
        elif options.accessOp == 'delete':
            self.accessOp = DELETE_TOPIC_ACCESS
        elif options.accessOp == 'update':
            self.accessOp = UPDATE_TOPIC_ACCESS
        elif options.accessOp == 'key_update':
            self.accessOp = UPDATE_TOPIC_ACCESS_KEY
        elif options.accessOp == 'priority_update':
            self.accessOp = UPDATE_TOPIC_ACCESS_PRIORITY
        elif options.accessOp == 'type_update':
            self.accessOp = UPDATE_TOPIC_ACCESS_TYPE
        elif options.accessOp == 'status':
            self.accessOp = UPDATE_TOPIC_AUTH_STATUS
        elif options.accessOp == 'clear':
            self.accessOp = CLEAR_TOPIC_ACCESS
        elif options.accessOp == 'list':
            self.accessOp = LIST_ONE_TOPIC_ACCESS
        elif options.accessOp == 'listall':
            self.accessOp = LIST_ALL_TOPIC_ACCESS

        self.topicName = options.topicName
        self.accessId = options.accessId
        self.accessKey = options.accessKey
        self.accessPriority = None
        if options.accessPriority == 'p0':
            self.accessPriority = ACCESS_PRIORITY_P0
        elif options.accessPriority == 'p1':
            self.accessPriority = ACCESS_PRIORITY_P1
        elif options.accessPriority == 'p2':
            self.accessPriority = ACCESS_PRIORITY_P2
        elif options.accessPriority == 'p3':
            self.accessPriority = ACCESS_PRIORITY_P3
        elif options.accessPriority == 'p4':
            self.accessPriority = ACCESS_PRIORITY_P4
        elif options.accessPriority == 'p5':
            self.accessPriority = ACCESS_PRIORITY_P5

        self.accessType = None
        if options.accessType == 'read_only':
            self.accessType = TOPIC_ACCESS_READ
        elif options.accessType == 'read_write':
            self.accessType = TOPIC_ACCESS_READ_WRITE
        elif options.accessType == "none":
            self.accessType = TOPIC_ACCESS_NONE

        self.checkStatus = None
        if options.checkStatus == 'on':
            self.checkStatus = TOPIC_AUTH_CHECK_ON
        else:
            self.checkStatus = TOPIC_AUTH_CHECK_OFF

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errMsg = self.adminDelegate.topicAclManage(
            accessOp=self.accessOp, topicName=self.topicName,
            accessId=self.accessId, accessKey=self.accessKey,
            accessPriority=self.accessPriority,
            accessType=self.accessType,
            checkStatus=self.checkStatus)

        if not ret:
            print "topic acl manage failed"
            return ret, response, 1

        print "topic acl manage success"
        if self.accessOp == LIST_ONE_TOPIC_ACCESS or self.accessOp == LIST_ALL_TOPIC_ACCESS:
            self.parseAndPrintTopicAcl(response)

        return ret, response, 0

    def parseAndPrintTopicAcl(self, response):
        topicAclDatas = response.topicAclDatas.allTopicAclData
        toFormatStr = '{"topicName":"%s", "checkStatus":"%s", "topicAccessInfos":[%s]}'
        for topicAclData in topicAclDatas:
            accessInfoList = []
            accessInfoStr = '{"accessId":"%s", "accessKey":"%s", "priority":"%s", "accessType":"%s"}'
            for topicAccessInfo in topicAclData.topicAccessInfos:
                oneItem = accessInfoStr % (topicAccessInfo.accessAuthInfo.accessId,
                                           topicAccessInfo.accessAuthInfo.accessKey,
                                           self._accessPriorityToStr(topicAccessInfo.accessPriority),
                                           self._accessTypeToStr(topicAccessInfo.accessType))
                accessInfoList.append(oneItem)
            accessInfosStr = ",".join(accessInfoList)
            oneItem = toFormatStr % (topicAclData.topicName,
                                     self._checkStatusToStr(topicAclData.checkStatus),
                                     accessInfosStr)
            print oneItem

    def _accessPriorityToStr(self, priority):
        if priority == ACCESS_PRIORITY_P0:
            return "p0"
        elif priority == ACCESS_PRIORITY_P1:
            return "p1"
        elif priority == ACCESS_PRIORITY_P2:
            return "p2"
        elif priority == ACCESS_PRIORITY_P3:
            return "p3"
        elif priority == ACCESS_PRIORITY_P4:
            return "p4"
        elif priority == ACCESS_PRIORITY_P5:
            return "p5"

    def _accessTypeToStr(self, type):
        if type == TOPIC_ACCESS_NONE:
            return "none"
        elif type == TOPIC_ACCESS_READ:
            return "read_only"
        elif type == TOPIC_ACCESS_READ_WRITE:
            return "read_write"

    def _checkStatusToStr(self, status):
        if status == TOPIC_AUTH_CHECK_ON:
            return "on"
        else:
            return "off"
