#!/bin/env python

import arpc.python.rpc_impl_async as rpc_impl
import swift.protocol.AdminRequestResponse_pb2 as swift_proto_admin
import swift.protocol.Control_pb2 as swift_proto_control
import swift.protocol.ErrCode_pb2 as swift_proto_errcode
import swift.protocol.Common_pb2 as swift_proto_common
from swift.protocol.Common_pb2 import *
import swift_common_define
import swift_delegate_base
import local_file_util
import tempfile
import time


def getIndexVal(item, index, expectSize, separator=';'):
    itemVec = item.split(separator)
    if expectSize != len(itemVec):
        raise Exception('size not equal, expect[%d] actual[%d]',
                        expectSize, len(itemVec))
    else:
        return itemVec[index]


class SwiftAdminDelegate(swift_delegate_base.SwiftDelegateBase):
    def __init__(self, fileUtil, zkRoot, adminLeaderIp=None, username=None,
                 passwd=None, accessId=None, accessKey=None):
        super(SwiftAdminDelegate, self).__init__(username, passwd, accessId, accessKey)
        self.fileUtil = fileUtil
        self.zkRoot = zkRoot
        self.adminLeaderIp = adminLeaderIp
        self.serverSpec = None

    def setServerSpec(self, serverSpec):
        self.serverSpec = serverSpec

    def createTopic(self, topicName, partCnt, rangeCnt=None, partBufSize=None,
                    partMinBufSize=None, partMaxBufSize=None,
                    partResource=None, partLimit=None, topicMode=None,
                    needFieldFilter=None, obsoleteFileTimeInterval=None,
                    reservedFileCount=None, partFileBufSize=None,
                    deleteTopicData=None, securityCommitTime=None,
                    securityCommitData=None, compressMsg=None, compressThres=None,
                    dfsRoot=None, topicGroup=None, extendDfsRoot=None, expiredTime=None,
                    owners=None, needSchema=None, schemaVersions=None,
                    requestTimeout=None, sealed=None, topicType=None,
                    physicTopicLst=None, enableTTLDel=None, enableLongPolling=None,
                    versionControl=None, enableMergeData=None, permitUser=None, readNotCommmitMsg=None):
        request = swift_proto_admin.TopicCreationRequest()
        request.topicName = topicName
        request.partitionCount = partCnt
        if rangeCnt is not None:
            request.rangeCountInPartition = rangeCnt
        if partBufSize is not None:
            request.partitionBufferSize = partBufSize
        if partMinBufSize is not None:
            request.partitionMinBufferSize = partMinBufSize
        if partMaxBufSize is not None:
            request.partitionMaxBufferSize = partMaxBufSize
        if partResource is not None:
            request.resource = partResource
        if partLimit is not None:
            request.partitionLimit = partLimit
        if topicMode is not None:
            request.topicMode = topicMode
        if needFieldFilter is not None:
            request.needFieldFilter = needFieldFilter
        if obsoleteFileTimeInterval is not None:
            request.obsoleteFileTimeInterval = obsoleteFileTimeInterval
        if reservedFileCount is not None:
            request.reservedFileCount = reservedFileCount
        if partFileBufSize is not None:
            request.partitionFileBufferSize = partFileBufSize
        if deleteTopicData is not None:
            request.deleteTopicData = deleteTopicData
        if securityCommitTime is not None:
            request.maxWaitTimeForSecurityCommit = securityCommitTime
        if securityCommitData is not None:
            request.maxDataSizeForSecurityCommit = securityCommitData
        if compressMsg is not None:
            request.compressMsg = compressMsg
        if compressThres is not None:
            request.compressThres = compressThres
        if dfsRoot is not None:
            request.dfsRoot = dfsRoot
        if topicGroup is not None:
            request.topicGroup = topicGroup
        if extendDfsRoot is not None:
            dfsRoots = extendDfsRoot.split(',')
            for path in dfsRoots:
                request.extendDfsRoot.append(path)
        if expiredTime is not None:
            request.topicExpiredTime = expiredTime
        if owners is not None:
            request.owners.extend(owners.split(','))
        if needSchema is not None:
            request.needSchema = needSchema
            if schemaVersions is not None:
                versions = schemaVersions.split(',')
                for ver in versions:
                    request.schemaVersions.append(long(ver))
        if requestTimeout is not None:
            request.timeout = requestTimeout
        if sealed is not None:
            request.sealed = sealed
        if topicType is not None:
            request.topicType = topicType
        if physicTopicLst is not None:
            request.physicTopicLst.extend(physicTopicLst.split(','))
        if enableTTLDel is not None:
            request.enableTTLDel = enableTTLDel
        if enableLongPolling is not None:
            request.enableLongPolling = enableLongPolling
        if versionControl is not None:
            request.versionControl = versionControl
        if enableMergeData is not None:
            request.enableMergeData = enableMergeData
        if permitUser:
            request.permitUser.extend(permitUser.split(","))
        if readNotCommmitMsg is not None:
            request.readNotCommittedMsg = readNotCommmitMsg

        return self._run(swift_common_define.SWIFT_METHOD_CREATE_TOPIC, request)

    def createTopicBatch(self, topicNames, partCnt, rangeCnt=None,
                         partMinBufSize=None, partMaxBufSize=None,
                         partResource=None, partLimit=None, topicMode=None,
                         needFieldFilter=None, obsoleteFileTimeInterval=None,
                         reservedFileCount=None, partFileBufSize=None,
                         deleteTopicData=None, securityCommitTime=None,
                         securityCommitData=None, compressMsg=None, compressThres=None,
                         dfsRoot=None, topicGroup=None, extendDfsRoot=None, expiredTime=None,
                         owners=None, needSchema=None, sealed=None, topicType=None,
                         physicTopicLst=None, enableTTLDel=None, permitUsers=None, readNotCommmitMsg=None):
        batchRequest = swift_proto_admin.TopicBatchCreationRequest()
        topicLst = topicNames.split(';')
        topicNum = len(topicLst)
        for index in range(topicNum):
            request = batchRequest.topicRequests.add()
            request.topicName = topicLst[index]
            request.partitionCount = int(getIndexVal(partCnt, index, topicNum))
            if rangeCnt is not None:
                request.rangeCountInPartition = int(getIndexVal(rangeCnt, index,
                                                                topicNum))
            if partMinBufSize is not None:
                request.partitionMinBufferSize = int(getIndexVal(partMinBufSize, index,
                                                                 topicNum))
            if partMaxBufSize is not None:
                request.partitionMaxBufferSize = int(getIndexVal(partMaxBufSize, index,
                                                                 topicNum))
            if partResource is not None:
                request.resource = int(getIndexVal(partResource, index,
                                                   topicNum))
            if partLimit is not None:
                request.partitionLimit = int(getIndexVal(partLimit, index, topicNum))
            if topicMode is not None:
                mode = getIndexVal(topicMode, index, topicNum)
                if mode == "normal_mode":
                    request.topicMode = TOPIC_MODE_NORMAL
                elif mode == "security_mode":
                    request.topicMode = TOPIC_MODE_SECURITY
                elif mode == "memory_only_mode":
                    request.topicMode = TOPIC_MODE_MEMORY_ONLY
                elif mode == "memory_prefer_mode":
                    request.topicMode = TOPIC_MODE_MEMORY_PREFER
            if needFieldFilter is not None:
                need = getIndexVal(needFieldFilter, index, topicNum)
                if 'true' == need.lower():
                    request.needFieldFilter = True
            if needSchema is not None:
                need = getIndexVal(needSchema, index, topicNum)
                if 'true' == need.lower():
                    if request.needFieldFilter:
                        raise Exception('cannot set both fieldFilter and needSchema')
                    else:
                        request.needSchema = True
            if sealed is not None:
                need = getIndexVal(sealed, index, topicNum)
                if 'true' == need.lower():
                    request.sealed = True
            if topicType is not None:
                tpType = getIndexVal(topicType, index, topicNum)
                if tpType.lower() == 'topic_type_normal':
                    request.topicType = TOPIC_TYPE_NORMAL
                elif tpType.lower() == 'topic_type_physic':
                    request.topicType = TOPIC_TYPE_PHYSIC
                elif tpType.lower() == 'topic_type_logic':
                    request.topicType = TOPIC_TYPE_LOGIC
                elif tpType.lower() == 'topic_type_logic_physic':
                    request.topicType = TOPIC_TYPE_LOGIC_PHYSIC
            if enableTTLDel is not None:
                enable = getIndexVal(enableTTLDel, index, topicNum)
                if 'true' == enable.lower():
                    request.enableTTLDel = True
                elif 'false' == enable.lower():
                    request.enableTTLDel = False

            if obsoleteFileTimeInterval is not None:
                request.obsoleteFileTimeInterval = int(getIndexVal(obsoleteFileTimeInterval,
                                                                   index, topicNum))
            if reservedFileCount is not None:
                request.reservedFileCount = int(getIndexVal(reservedFileCount, index,
                                                            topicNum))
            if partFileBufSize is not None:
                request.partitionFileBufferSize = int(getIndexVal(partFileBufSize,
                                                                  index, topicNum))
            if deleteTopicData is not None:
                isDel = getIndexVal(deleteTopicData, index, topicNum)
                if 'true' == isDel.lower():
                    request.deleteTopicData = True

            if securityCommitTime is not None:
                request.maxWaitTimeForSecurityCommit = getIndexVal(securityCommitTime,
                                                                   index, topicNum)
            if securityCommitData is not None:
                request.maxDataSizeForSecurityCommit = getIndexVal(securityCommitData,
                                                                   index, topicNum)
            if compressMsg is not None:
                compress = getIndexVal(compressMsg, index, topicNum)
                if 'false' == compress.lower():
                    request.compressMsg = False
            if compressThres is not None:
                request.compressThres = int(getIndexVal(compressThres, index, topicNum))
            if dfsRoot is not None:
                request.dfsRoot = getIndexVal(dfsRoot, index, topicNum)
            if topicGroup is not None:
                request.topicGroup = getIndexVal(topicGroup, index, topicNum)
            if extendDfsRoot is not None:
                edr = getIndexVal(extendDfsRoot, index, topicNum)
                dfsRoots = edr.split(',')
                for path in dfsRoots:
                    request.extendDfsRoot.append(path)
            if expiredTime is not None:
                request.topicExpiredTime = int(getIndexVal(expiredTime, index, topicNum))
            if owners is not None:
                own = getIndexVal(owners, index, topicNum)
                request.owners.extend(own.split(','))
            if permitUsers:
                permitUser = getIndexVal(permitUsers, index, topicNum)
                request.permitUser.extend(permitUser.split(","))
            if readNotCommmitMsg is not None:
                request.readNotCommittedMsg = readNotCommmitMsg

        return self._run(swift_common_define.SWIFT_METHOD_CREATE_TOPIC_BATCH, batchRequest)

    def modifyTopic(self, topicName, partResource=None, partLimit=None, topicGroup=None,
                    expiredTime=None, partMinBufSize=None, partMaxBufSize=None,
                    topicMode=None, obsoleteFileTimeInterval=None, reservedFileCount=None,
                    partCnt=None, owners=None, dfsRoot=None, extendDfsRoot=None,
                    rangeCountInPartition=None, sealed=None, topicType=None,
                    physicTopicLst=None, enableTTLDel=None, readSizeLimitSec=None,
                    enableLongPolling=None, securityCommitTime=None, securityCommitData=None,
                    enableMergeData=None, permitUser=None, versionControl=None, readNotCommmitMsg=None):
        request = swift_proto_admin.TopicCreationRequest()
        request.topicName = topicName
        if partResource is not None:
            request.resource = partResource
        if partLimit is not None:
            request.partitionLimit = partLimit
        if topicGroup is not None:
            request.topicGroup = topicGroup
        if expiredTime is not None:
            request.topicExpiredTime = expiredTime
        if partMinBufSize is not None:
            request.partitionMinBufferSize = partMinBufSize
        if partMaxBufSize is not None:
            request.partitionMaxBufferSize = partMaxBufSize
        if topicMode is not None:
            request.topicMode = topicMode
        if obsoleteFileTimeInterval is not None:
            request.obsoleteFileTimeInterval = obsoleteFileTimeInterval
        if reservedFileCount is not None:
            request.reservedFileCount = reservedFileCount
        if partCnt is not None:
            request.partitionCount = partCnt
        if owners is not None:
            request.owners.extend(owners.split(','))
        if dfsRoot is not None:
            request.dfsRoot = dfsRoot
        if extendDfsRoot is not None:
            request.extendDfsRoot.extend(extendDfsRoot.split(','))
        if rangeCountInPartition is not None:
            request.rangeCountInPartition = rangeCountInPartition
        if sealed is not None:
            request.sealed = sealed
        if topicType is not None:
            request.topicType = topicType
        if physicTopicLst is not None:
            request.physicTopicLst.extend(physicTopicLst.split(','))
        if enableTTLDel is not None:
            request.enableTTLDel = enableTTLDel
        if readSizeLimitSec is not None:
            request.readSizeLimitSec = readSizeLimitSec
        if enableLongPolling is not None:
            request.enableLongPolling = enableLongPolling
        if securityCommitTime is not None:
            request.maxWaitTimeForSecurityCommit = securityCommitTime
        if securityCommitData is not None:
            request.maxDataSizeForSecurityCommit = securityCommitData
        if enableMergeData is not None:
            request.enableMergeData = enableMergeData
        if permitUser:
            request.permitUser.extend(permitUser.split(","))
        if versionControl is not None:
            request.versionControl = versionControl
        if readNotCommmitMsg is not None:
            request.readNotCommittedMsg = readNotCommmitMsg
        return self._run(swift_common_define.SWIFT_METHOD_MODIFY_TOPIC, request)

    def transferPartition(self, transferInfo, groupName=None):
        request = swift_proto_admin.PartitionTransferRequest()
        if groupName is not None:
            request.groupName = groupName
        if transferInfo != 'all':
            infoVec = transferInfo.split(",")
            for info in infoVec:
                tmp = info.split(":")
                if len(tmp) == 0 or len(tmp) > 2:
                    return False, "", "transfer split failed"
                tranInfo = request.transferInfo.add()
                tranInfo.brokerRoleName = tmp[0]
                if (len(tmp) == 2):
                    tranInfo.ratio = float(tmp[1])
        return self._run(swift_common_define.SWIFT_METHOD_TRANSFER_PARTITION, request)

    def changeSlot(self, roleNames):
        request = swift_proto_admin.ChangeSlotRequest()
        roleNameVec = roleNames.split(",")
        if len(roleNameVec) == 0:
            return False, "", "empty role name"
        for roleName in roleNameVec:
            request.roleNames.append(roleName)
        return self._run(swift_common_define.SWIFT_METHOD_CHANGE_SLOT, request)

    def deleteTopic(self, topicName):
        request = swift_proto_admin.TopicDeletionRequest()
        request.topicName = topicName
        return self._run(swift_common_define.SWIFT_METHOD_DELETE_TOPIC, request)

    def deleteTopicBatch(self, topicNames):
        request = swift_proto_admin.TopicBatchDeletionRequest()
        for topicName in topicNames:
            request.topicNames.append(topicName)
        return self._run(swift_common_define.SWIFT_METHOD_DELETE_TOPIC_BATCH, request)

    def getAllTopicName(self):
        request = swift_proto_admin.EmptyRequest()
        return self._run(swift_common_define.SWIFT_METHOD_GET_ALL_TOPIC_NAME, request)

    def getTopicInfo(self, topicName, requestTimeout=None):
        request = swift_proto_admin.TopicInfoRequest()
        request.topicName = topicName
        request.src = "api"
        if requestTimeout is not None:
            request.timeout = requestTimeout
        return self._run(swift_common_define.SWIFT_METHOD_GET_TOPIC_INFO, request)

    def getAllTopicInfo(self, ):
        request = swift_proto_admin.EmptyRequest()
        return self._run(swift_common_define.SWIFT_METHOD_GET_ALL_TOPIC_INFO, request)

    def getAllTopicSimpleInfo(self, ):
        request = swift_proto_admin.EmptyRequest()
        return self._run(swift_common_define.SWIFT_METHOD_GET_ALL_TOPIC_SIMPLE_INFO, request)

    def getRoleAddress(self, roleType, roleStatus):
        request = swift_proto_admin.RoleAddressRequest()
        request.role = roleType
        request.status = roleStatus
        return self._run(swift_common_define.SWIFT_METHOD_GET_ROLE_ADDRESS, request)

    def getAllWorkerStatus(self):
        request = swift_proto_admin.EmptyRequest()
        return self._run(swift_common_define.SWIFT_METHOD_GET_ALL_WORKER_STATUS, request)

    def getWorkerError(self, time=None, count=None, level=None):
        request = swift_proto_admin.ErrorRequest()
        if time is not None:
            request.time = time
        if count is not None:
            request.count = count
        if level is not None:
            request.level = level
        return self._run(swift_common_define.SWIFT_METHOD_GET_WORKER_ERROR, request)

    def getPartitionError(self, time=None, count=None, level=None):
        request = swift_proto_admin.ErrorRequest()
        if time is not None:
            request.time = time
        if count is not None:
            request.count = count
        if level is not None:
            request.level = level
        return self._run(swift_common_define.SWIFT_METHOD_GET_PARTITION_ERROR, request)

    def updateBrokerConfig(self, configPath):
        request = swift_proto_admin.UpdateBrokerConfigRequest()
        request.configPath = configPath
        return self._run(swift_common_define.SWIFT_METHOD_UPDATE_BROKER_CONFIG, request)

    def rollbackBrokerConfig(self):
        request = swift_proto_admin.RollbackBrokerConfigRequest()
        return self._run(swift_common_define.SWIFT_METHOD_ROLLBACK_BROKER_CONFIG, request)

    def getAdminLeaderAddr(self):
        if self.adminLeaderIp:
            # just for function test case
            print "WARN: using user configured admin leader[%s], "\
                "just for function test case." % self.adminLeaderIp
            return "tcp:" + self.adminLeaderIp

        adminSpec = None
        adminStopped = True
        i = 0
        while (i < swift_common_define.GET_ADMIN_LEADER_TRY_TIMES):
            try:
                adminLeaderInfo = self.getAdminLeaderInfo()
            except BaseException:
                print "get admin leader info failed for[%d] times" % (i)
                i = i + 1
                time.sleep(0.5)
                continue

            if adminLeaderInfo:
                if adminLeaderInfo.HasField(swift_common_define.PROTO_ADDRESS):
                    adminSpec = adminLeaderInfo.address
                if adminLeaderInfo.HasField(swift_common_define.PROTO_SYS_STOP):
                    adminStopped = adminLeaderInfo.sysStop
                if (not adminStopped) and adminSpec:
                    return "tcp:" + adminSpec
            i = i + 1
            time.sleep(0.5)

        print "ERROR: could not get admin leader address! You should start swift system first!"
        return None

    def getAdminLeaderInfo(self):
        adminLeaderFile = self._getAdminLeaderInfoFile()
        content = self._getZfsFileContent(adminLeaderFile)
        if not content:
            return None
        leaderInfo = swift_proto_common.LeaderInfo()
        try:
            leaderInfo.ParseFromString(content)
        except Exception as e:
            print "ERROR: parse leaderInfo from string[%s] failed!" % content
            return None
        return leaderInfo

    def getAdminLeaderHistory(self):
        adminLeaderHisFile = self._getAdminLeaderHisFile()
        content = self._getZfsFileContent(adminLeaderHisFile)
        if not content:
            return None
        leaderHis = swift_proto_common.LeaderInfoHistory()
        try:
            leaderHis.ParseFromString(content)
        except Exception as e:
            print "ERROR: parse LeaderInfoHistory from string[%s] failed!" \
                % content
            return None
        return leaderHis

    def registerSchema(self, topicName, schema, version=None, cover=False):
        request = swift_proto_admin.RegisterSchemaRequest()
        request.topicName = topicName
        request.schema = schema
        if version is not None:
            request.version = int(version)
        request.cover = cover
        return self._run(swift_common_define.SWIFT_METHOD_REGISTER_SCHEMA, request)

    def getSchema(self, topicName, version):
        request = swift_proto_admin.GetSchemaRequest()
        request.topicName = topicName
        request.version = int(version)
        return self._run(swift_common_define.SWIFT_METHOD_GET_SCHEMA, request)

    def getBrokerStatus(self, roleName=None):
        request = swift_proto_admin.GetBrokerStatusRequest()
        if roleName is not None:
            request.roleName = roleName
        return self._run(swift_common_define.SWIFT_METHOD_GET_BROKER_STATUS, request)

    def getTopicRWTime(self, topicName=None):
        request = swift_proto_admin.GetTopicRWTimeRequest()
        if topicName is not None:
            request.topicName = topicName
        return self._run(swift_common_define.SWIFT_METHOD_GET_TOPIC_RWTIME, request)

    def getLastDeletedNoUseTopic(self):
        request = swift_proto_admin.EmptyRequest()
        return self._run(swift_common_define.SWIFT_METHOD_GET_LAST_DELETED_NOUSE_TOPIC, request)

    def getDeletedNoUseTopic(self, fileName):
        request = swift_proto_admin.DeletedNoUseTopicRequest()
        request.fileName = fileName
        return self._run(swift_common_define.SWIFT_METHOD_GET_DELETED_NOUSE_TOPIC, request)

    def getDeletedNoUseTopicFiles(self):
        request = swift_proto_admin.EmptyRequest()
        return self._run(swift_common_define.SWIFT_METHOD_GET_DELETED_NOUSE_TOPIC_FILES, request)

    def updateWriterVersion(self, topicName=None, writerName=None, majorVersion=None, minorVersion=None):
        request = swift_proto_admin.UpdateWriterVersionRequest()
        if topicName is not None:
            request.topicWriterVersion.topicName = topicName
        if majorVersion is not None:
            request.topicWriterVersion.majorVersion = int(majorVersion)
        writerVersion = request.topicWriterVersion.writerVersions.add()
        if writerName is not None:
            writerVersion.name = writerName
            writerVersion.version = int(minorVersion)
        return self._run(swift_common_define.SWIFT_METHOD_UPDATE_WRITER_VERSION, request)

    def turnToMaster(self, version):
        request = swift_proto_admin.TurnToMasterRequest()
        request.targetVersion = int(version)
        return self._run(swift_common_define.SWIFT_METHOD_TURN_TO_MASTER, request)

    def topicAclManage(self, accessOp, topicName=None, accessId=None,
                       accessKey=None, accessPriority=None,
                       accessType=None, checkStatus=None):
        request = swift_proto_admin.TopicAclRequest()
        request.accessOp = accessOp
        if topicName is not None:
            request.topicName = topicName
        if accessId is not None:
            request.topicAccessInfo.accessAuthInfo.accessId = accessId
        if accessKey is not None:
            request.topicAccessInfo.accessAuthInfo.accessKey = accessKey
        if accessPriority is not None:
            request.topicAccessInfo.accessPriority = accessPriority
        if accessType is not None:
            request.topicAccessInfo.accessType = accessType
        if checkStatus is not None:
            request.authCheckStatus = checkStatus
        return self._run(swift_common_define.SWIFT_METHOD_TOPIC_ACL_MANAGE, request)

    def _getAdminLeaderInfoFile(self):
        return self.fileUtil.normalizeDir(self.zkRoot) \
            + "admin/leader_info"

    def _getAdminLeaderHisFile(self):
        return self.fileUtil.normalizeDir(self.zkRoot) \
            + "admin/leader_history"

    def _getZfsFileContent(self, zfsPath):
        if not self.fileUtil.exists(zfsPath):
            return None

        tmpFile = tempfile.NamedTemporaryFile()
        if not self.fileUtil.copy(zfsPath, tmpFile.name, True):
            tmpFile.close()
            return None

        localFileUtil = local_file_util.LocalFileUtil()
        content = localFileUtil.read(tmpFile.name)
        tmpFile.close()
        return content

    def _run(self, method, request):
        request.authentication.username = self.username
        request.authentication.passwd = self.passwd
        request.authentication.accessAuthInfo.accessId = self.accessId
        request.authentication.accessAuthInfo.accessKey = self.accessKey
        request.versionInfo.supportAuthentication = True
        serverSpec = self.getAdminLeaderAddr()
        print "admin leader:", serverSpec
        if not serverSpec:
            return False, None, "get admin leader address failed."
        response = self._callMethod(serverSpec, method, request)
        ret, errMsg = self._checkErrorInfo(response, serverSpec)
        if not ret:
            return False, response, errMsg
        return True, response, errMsg

    def _callMethod(self, serverSpec, method, request):
        adminStub = rpc_impl.ServiceStubWrapperFactory.createServiceStub(
            swift_proto_control.Controller_Stub, serverSpec)
        if not adminStub:
            print "ERROR: CreateServiceStub failed!"
            return None

        try:
            response = adminStub.syncCall(method, request,
                                          timeOut=swift_common_define.ARPC_TIME_OUT)
            return response
        except rpc_impl.ReplyTimeOutError as e:
            print "ERROR: call method[%s] in server[%s] time out!"\
                % (method, serverSpec)
            return None
