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
#include "master/SlotMapper.h"
#include "carbon/Log.h"
#include "master/SlotUtil.h"

using namespace std;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, SlotMapper);

SlotMapper::SlotMapper() {
}

SlotMapper::~SlotMapper() {
}

void SlotMapper::mapping(const SlotInfoMap &slotInfos,
                         const map<tag_t, WorkerNodeVect> &workerNodes)
{
    map<tag_t, SlotInfoMap> tagSlotInfoMap;
    for (SlotInfoMap::const_iterator slotInfoIt = slotInfos.begin();
         slotInfoIt != slotInfos.end(); slotInfoIt++)
    {
        const SlotInfo &slotInfo = slotInfoIt->second;
        tagSlotInfoMap[slotInfo.role][slotInfo.slotId] = slotInfo;
    }

    for (map<tag_t, WorkerNodeVect>::const_iterator it = workerNodes.begin();
         it != workerNodes.end(); it++)
    {
        const tag_t &tag = it->first;
        SlotMapper::mapping(tagSlotInfoMap[tag], it->second);
    }
}

void SlotMapper::mapping(const SlotInfoMap &slotInfos,
                         const WorkerNodeVect &workerNodes)
{
    set<SlotId> assignedSlotIds;
    WorkerNodeVect unAssignedWorkerNodes;
    for (size_t i = 0; i < workerNodes.size(); i++) {
        WorkerNodePtr workerNodePtr = workerNodes[i];
        if (workerNodePtr->isUnAssignedSlot()) {
            unAssignedWorkerNodes.push_back(workerNodePtr);
            continue;
        }
        
        SlotId slotId = workerNodePtr->getSlotId();
        if (slotInfos.find(slotId) != slotInfos.end()) {
            assignedSlotIds.insert(slotId);
        }
    }

    for (size_t i = 0; i < unAssignedWorkerNodes.size(); i++) {
        WorkerNodePtr workerNodePtr = unAssignedWorkerNodes[i];
        for (SlotInfoMap::const_iterator it = slotInfos.begin();
             it != slotInfos.end(); it++)
        {
            const SlotInfo &slotInfo = it->second;
            const SlotId &slotId = it->first;
            if (assignedSlotIds.find(slotId) == assignedSlotIds.end()) {
                workerNodePtr->assignSlot(slotInfo);
                assignedSlotIds.insert(slotId);
                break;
            }
        }
    }
}

END_CARBON_NAMESPACE(master);

