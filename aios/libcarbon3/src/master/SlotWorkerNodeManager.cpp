/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "master/SlotWorkerNodeManager.h"

using namespace autil;

BEGIN_CARBON_NAMESPACE(master);

SlotWorkerNodeManager::SlotWorkerNodeManager() {

}

SlotWorkerNodeManager::~SlotWorkerNodeManager() {

}

void SlotWorkerNodeManager::storeGroupStatusList(
        const std::vector<GroupStatus> &allGroupStatus,
        bool all) {
    ScopedWriteLock lock(_lock);
    if (all) {
        _slotWorkerNodeStatusMap.clear();
    }

    auto groupStatusIter = allGroupStatus.cbegin();
    for ( ; groupStatusIter != allGroupStatus.cend(); ++groupStatusIter) {
        const GroupStatus& groupStatus = *groupStatusIter;
        auto roleStatusIter = groupStatus.roles.cbegin();
        for ( ; roleStatusIter != groupStatus.roles.cend(); ++roleStatusIter) {
            const RoleStatus& roleStatus = roleStatusIter->second;
            auto replicaStatusIter = roleStatus.nodes.cbegin();
            for ( ; replicaStatusIter != roleStatus.nodes.cend(); ++replicaStatusIter) {
                const ReplicaNodeStatus& replicaNodeStatus = *replicaStatusIter;
                if (!replicaNodeStatus.curWorkerNodeStatus.ip.empty()) {
                    const SlotId& slotId = replicaNodeStatus.curWorkerNodeStatus.slotInfo.slotId;
                    _slotWorkerNodeStatusMap[slotId.slaveAddress][slotId.id] = replicaNodeStatus.curWorkerNodeStatus;
                }
                if (!replicaNodeStatus.backupWorkerNodeStatus.ip.empty()) {
                    const SlotId& slotId = replicaNodeStatus.backupWorkerNodeStatus.slotInfo.slotId;
                    _slotWorkerNodeStatusMap[slotId.slaveAddress][slotId.id] = replicaNodeStatus.backupWorkerNodeStatus;
                }
            }
        }
    }
}

std::vector<WorkerNodeStatus>  SlotWorkerNodeManager::retrieveWorkerNodes(
        const std::string& slaveAddress) const {
    ScopedReadLock lock(_lock);
    std::vector<WorkerNodeStatus> workerNodeStatusList;

    auto iter = _slotWorkerNodeStatusMap.find(slaveAddress);
    if (iter == _slotWorkerNodeStatusMap.cend()) {
        return workerNodeStatusList;
    }
    
    auto idIter = iter->second.cbegin();
    for ( ; idIter != iter->second.cend(); ++idIter) {
        workerNodeStatusList.push_back(idIter->second);
    }
    return workerNodeStatusList;
}

std::vector<WorkerNodeStatus> SlotWorkerNodeManager::retrieveWorkerNode(
        const std::vector<SlotId>& slotIds) const {
    ScopedReadLock lock(_lock);
    std::vector<WorkerNodeStatus> workerNodeStatusList;
    
    for (uint32_t i = 0; i < slotIds.size(); ++i) {
        const SlotId& slotId = slotIds[i];
        auto iter = _slotWorkerNodeStatusMap.find(slotId.slaveAddress);
        if ( iter == _slotWorkerNodeStatusMap.cend()) {
            continue;
        }
        const SlotIdWorkerNodeStatusMap& idMap = iter->second;
        auto iter1 = idMap.find(slotId.id);
        if (iter1 == idMap.cend()) {
            continue;
        }
        workerNodeStatusList.push_back(iter1->second);
    }
    return workerNodeStatusList;
}


END_CARBON_NAMESPACE(master);