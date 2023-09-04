#!/bin/env python

import swift_common_define
import swift_util
import swift.protocol.Common_pb2 as swift_proto_common


class PartitionCount:
    partWaiting = 0
    partStarting = 0
    partRunning = 0
    partStopping = 0
    partRecovering = 0
    partNone = 0


class TopicInfoAnalyzer:
    def getPartitionCount(self, topicInfo):
        partCount = PartitionCount()
        for partInfo in topicInfo.partitionInfos:
            if partInfo.status == swift_proto_common.PARTITION_STATUS_WAITING:
                partCount.partWaiting = partCount.partWaiting + 1
            elif partInfo.status == swift_proto_common.PARTITION_STATUS_STARTING:
                partCount.partStarting = partCount.partStarting + 1
            elif partInfo.status == swift_proto_common.PARTITION_STATUS_RUNNING:
                partCount.partRunning = partCount.partRunning + 1
            elif partInfo.status == swift_proto_common.PARTITION_STATUS_STOPPING:
                partCount.partStopping = partCount.partStopping + 1
            elif partInfo.status == swift_proto_common.PARTITION_STATUS_RECOVERING:
                partCount.partRecovering = partCount.partRecovering + 1
            else:
                partCount.partNone = partCount.partNone + 1
        return partCount

    def getSortedPartitionInfo(self, topicInfo, sortKey):
        cmpFunc = None
        if sortKey == swift_common_define.PART_SORT_PART_STATUS:
            cmpFunc = self._partitionCmpByStatus
        elif sortKey == swift_common_define.PART_SORT_BROKER_ADDRESS:
            cmpFunc = self._partitionCmpByBrokerAddress
        else:
            cmpFunc = self._partitionCmpByPartId

        partInfoList = [partInfo for partInfo in topicInfo.partitionInfos]
        partInfoList.sort(cmpFunc)
        return partInfoList

    def _partitionCmpByPartId(self, partA, partB):
        return cmp(int(partA.id), int(partB.id))

    def _partitionCmpByBrokerAddress(self, partA, partB):
        return cmp(partA.brokerAddress, partB.brokerAddress)

    def _partitionCmpByStatus(self, partA, partB):
        ret = partA.status - partB.status
        if ret == 0:
            return self._partitionCmpByBrokerAddress(partA, partB)
        return ret


class WorkerStatusAnalyzer:
    def getAdminWorkerStatus(self, workersStatus):
        return self._getWorkerStatusByRole(workersStatus,
                                           swift_proto_common.ROLE_TYPE_ADMIN)

    def getBrokerWorkerStatus(self, workersStatus):
        return self._getWorkerStatusByRole(workersStatus,
                                           swift_proto_common.ROLE_TYPE_BROKER)

    def getSortedWorkerStatus(self, workersStatus):
        workerStatusList = [status for status in workersStatus]
        workerStatusList.sort(self._cmpWorkerStatusByRoleName)
        return workerStatusList

    def _cmpWorkerStatusByRoleName(self, workerA, workerB):
        roleA = swift_util.SwiftUtil.protoEnumToString(workerA.current, 'role')
        roleB = swift_util.SwiftUtil.protoEnumToString(workerB.current, 'role')
        ret = cmp(roleA, roleB)
        if ret == 0:
            ret = cmp(workerA.current.address, workerB.current.address)
        return ret

    def _getWorkerStatusByRole(self, workersStatus, roleType):
        status = []
        for workerStatus in workersStatus:
            if workerStatus.HasField("current"):  # HeartbeatInfo
                currentHearbeat = workerStatus.current
                if currentHearbeat.HasField("role") and\
                        currentHearbeat.role == roleType:
                    status.append(workerStatus)

        return status
