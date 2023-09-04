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
#ifndef CARBON_ROLEEXECUTOR_H
#define CARBON_ROLEEXECUTOR_H

#include "carbon/CommonDefine.h"
#include "common/common.h"
#include "carbon/Log.h"
#include "master/HippoAdapter.h"
#include "master/WorkerNode.h"
#include "master/ExtVersionedPlan.h"

BEGIN_CARBON_NAMESPACE(master);

struct ScheduleInfo {
    ScheduleInfo() {
        count = -1;
        minHealthCount = -1;
    }
    
    int32_t minHealthCount;
    int32_t count;
};

class RoleExecutor
{
public:
    RoleExecutor(const HippoAdapterPtr &hippoAdapterPtr);
    ~RoleExecutor();
private:
    RoleExecutor(const RoleExecutor &);
    RoleExecutor& operator=(const RoleExecutor &);
public:
    void execute(const groupid_t &groupId,
                 const roleid_t &roleId,
                 const WorkerNodeVect &allWorkerNodes,
                 const std::map<version_t, ExtVersionedPlan> &versionedPlans,
                 const version_t &latestVersion,
                 const SlotInfoMap &slotInfoMap,
                 const ScheduleInfo &scheduleInfo);

public:
    void getResourceRequests(
            const groupid_t &groupId,
            const roleid_t &roleId,
            const WorkerNodeVect &allWorkerNodes,
            const std::map<version_t, ExtVersionedPlan> &versionedPlans,
            const version_t &latestVersion,
            const SlotInfoMap &slotInfoMap,
            const ScheduleInfo &scheduleInfo,
            std::map<tag_t, ResourceRequest> *resourceRequestMap,
            std::map<SlotId, ReleasePreference> *releaseSlots);

    void groupWorkerNodesByTag(
        const WorkerNodeVect &allWorkerNodes,
        const std::map<version_t, ExtVersionedPlan> &versionedPlans,
        std::map<tag_t, WorkerNodeVect> *tagedWorkerNodes);

    void groupVersionedPlanByTag(
            const std::map<version_t, ExtVersionedPlan> &versionedPlans,
            const version_t &latestVersion,
            std::map<tag_t, ExtVersionedPlan> *tagedVersionedPlans);

    void genResourceRequests(
            const groupid_t &groupId,
            const roleid_t &roleId,
            const std::map<tag_t, WorkerNodeVect> &tagedWorkerNodes,
            const std::map<tag_t, ExtVersionedPlan> &tagedVersionedPlans,
            const ScheduleInfo &scheduleInfo,
            std::map<tag_t, ResourceRequest> *resourceRequestMap);

    int32_t countSlotsNeedAllocate(
            const tag_t &tag,
            const std::map<tag_t, WorkerNodeVect> &tagedWorkerNodes);

    void getReleaseSlots(
            const std::map<tag_t, WorkerNodeVect> &tagedWorkerNodes,
            const SlotInfoMap &slotInfoMap,
            std::map<SlotId, ReleasePreference> *releaseSlots);

    void getLaunchPlans(const WorkerNodeVect &allWorkerNodes,
                        std::map<SlotId, LaunchPlan> *curLaunchPlanMap,
                        std::map<SlotId, LaunchPlan> *finalLaunchPlanMap);

public: // public for test
    void fillRoleUidInLaunchPlan(const nodeid_t &nodeId, LaunchPlan *launchPlan);
private:
    bool checkEnvExisted(const std::vector<hippo::PairType> &envs, const std::string &key);

private:
    HippoAdapterPtr _hippoAdapterPtr;
private:
    CARBON_LOG_DECLARE();
};

CARBON_TYPEDEF_PTR(RoleExecutor);

END_CARBON_NAMESPACE(master);

#endif //CARBON_ROLEEXECUTOR_H
