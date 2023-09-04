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
#ifndef CARBON_MASTER_SLOTWORKERNODEMANAGER_H
#define CARBON_MASTER_SLOTWORKERNODEMANAGER_H

#include "common/common.h"
#include "carbon/Status.h"
#include "autil/Lock.h"

BEGIN_CARBON_NAMESPACE(master);

class SlotWorkerNodeManager {
public: 
    SlotWorkerNodeManager();
    virtual ~SlotWorkerNodeManager();

private:
    SlotWorkerNodeManager(const SlotWorkerNodeManager &);
    SlotWorkerNodeManager& operator=(const SlotWorkerNodeManager &);

public:
    void storeGroupStatusList(
        const std::vector<GroupStatus> &allGroupStatus,
        bool all);
    
    std::vector<WorkerNodeStatus> retrieveWorkerNodes(
        const std::string& slaveAddress) const;

    std::vector<WorkerNodeStatus> retrieveWorkerNode(
        const std::vector<SlotId>& slotIds) const;

private:
    mutable autil::ReadWriteLock _lock;
    typedef std::map<int32_t, WorkerNodeStatus> SlotIdWorkerNodeStatusMap;
    typedef std::map<std::string, SlotIdWorkerNodeStatusMap> SlotWorkerNodeStatusMap;
    SlotWorkerNodeStatusMap _slotWorkerNodeStatusMap;
};

CARBON_TYPEDEF_PTR(SlotWorkerNodeManager)
END_CARBON_NAMESPACE(master);
#endif