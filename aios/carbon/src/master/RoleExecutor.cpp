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
#include "master/RoleExecutor.h"
#include "master/GlobalVariable.h"
#include "carbon/Log.h"
#include "master/SlotUtil.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "master/Flag.h"

using namespace std;
using namespace autil;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, RoleExecutor);

RoleExecutor::RoleExecutor(const HippoAdapterPtr &hippoAdapterPtr) {
    _hippoAdapterPtr = hippoAdapterPtr;
}

RoleExecutor::~RoleExecutor() {
}

void RoleExecutor::execute(
        const groupid_t &groupId,
        const roleid_t &roleId,
        const WorkerNodeVect &allWorkerNodes,
        const map<version_t, ExtVersionedPlan> &versionedPlans,
        const version_t &latestVersion,
        const SlotInfoMap &slotInfoMap,
        const ScheduleInfo &scheduleInfo)
{
    map<tag_t, ResourceRequest> resourceRequestMap;
    map<SlotId, ReleasePreference> releaseSlots;
    getResourceRequests(groupId, roleId, allWorkerNodes, versionedPlans,
                        latestVersion, slotInfoMap, scheduleInfo, &resourceRequestMap,
                        &releaseSlots);

    _hippoAdapterPtr->allocateSlots(resourceRequestMap, releaseSlots);

    map<SlotId, LaunchPlan> curLaunchPlanMap, finalLaunchPlanMap;
    getLaunchPlans(allWorkerNodes, &curLaunchPlanMap, &finalLaunchPlanMap);

    _hippoAdapterPtr->launchSlots(curLaunchPlanMap, finalLaunchPlanMap);
}

void RoleExecutor::getResourceRequests(
        const groupid_t &groupId,
        const roleid_t &roleId,
        const WorkerNodeVect &allWorkerNodes,
        const map<version_t, ExtVersionedPlan> &versionedPlans,
        const version_t &latestVersion,
        const SlotInfoMap &slotInfoMap,
        const ScheduleInfo &scheduleInfo,
        map<tag_t, ResourceRequest> *resourceRequestMap,
        map<SlotId, ReleasePreference> *releaseSlots)
{
    /* after upgrade, each role should only have one resourcetag,
     * and this resourcetag should corresponding to the latest resource plan.
     * but when upgrading, there is still two resourcetag.
     */
    map<tag_t, WorkerNodeVect> tagedWorkerNodes;
    groupWorkerNodesByTag(allWorkerNodes, versionedPlans, &tagedWorkerNodes);
    map<tag_t, ExtVersionedPlan> tagedVersionedPlans;
    groupVersionedPlanByTag(versionedPlans, latestVersion, &tagedVersionedPlans);

    genResourceRequests(groupId, roleId, tagedWorkerNodes, tagedVersionedPlans,
                        scheduleInfo, resourceRequestMap);

    getReleaseSlots(tagedWorkerNodes, slotInfoMap, releaseSlots);
}

void RoleExecutor::groupWorkerNodesByTag(
        const WorkerNodeVect &allWorkerNodes,
        const map<version_t, ExtVersionedPlan> &versionedPlans,
        map<tag_t, WorkerNodeVect> *tagedWorkerNodes)
{
    tagedWorkerNodes->clear();
    for (size_t i = 0; i < allWorkerNodes.size(); i++) {
        const WorkerNodePtr &workerNodePtr = allWorkerNodes[i];
        auto curResTag = workerNodePtr->getCurResourceTag();
        if (curResTag.empty()) {
            CARBON_LOG(DEBUG, "empty resource tag, should not appear except in unittest");
            continue;
        }
        (*tagedWorkerNodes)[curResTag].push_back(workerNodePtr);
    }
}

void RoleExecutor::groupVersionedPlanByTag(
        const map<version_t, ExtVersionedPlan> &versionedPlans,
        const version_t &latestVersion,
        map<tag_t, ExtVersionedPlan> *tagedVersionedPlans)
{
    map<version_t, ExtVersionedPlan>::const_iterator it =
        versionedPlans.begin();
    for (it = versionedPlans.begin(); it != versionedPlans.end();
         it ++)
    {
        (*tagedVersionedPlans)[it->second.resourceTag] = it->second;
    }
    /* make sure use latest resource plan, after this version, above code should
     * be removed.
     */
    const auto &pItr = versionedPlans.find(latestVersion);
    assert(pItr != versionedPlans.end());
    if (pItr == versionedPlans.end()) {
        CARBON_LOG(ERROR, "latestversion %s not in versionedPlans",
                   latestVersion.c_str());
        return;
    }
    const auto &latestPlan = pItr->second;
    (*tagedVersionedPlans)[latestPlan.resourceTag] = latestPlan;
}

void RoleExecutor::genResourceRequests(
        const groupid_t &groupId,
        const roleid_t &roleId,
        const map<tag_t, WorkerNodeVect> &tagedWorkerNodes,
        const map<tag_t, ExtVersionedPlan> &tagedVersionedPlans,
        const ScheduleInfo &scheduleInfo,
        map<tag_t, ResourceRequest> *resourceRequestMap)
{
    for (const auto &kv:tagedVersionedPlans)
    {
        const tag_t &tag = kv.first;
        const VersionedPlan &plan = kv.second.plan;
        int32_t count = countSlotsNeedAllocate(tag, tagedWorkerNodes);
        ResourceRequest &resourceRequest = (*resourceRequestMap)[tag];
        resourceRequest.requirementId = kv.second.resourceChecksum;
        resourceRequest.count = count;
        resourceRequest.resourcePlan = plan.resourcePlan;
        if (master::Flag::isSlotOnC2()) {
            resourceRequest.resourcePlan.metaTags[METATAGS_BIZINFO_CARBON_GROUP_KEY] = groupId;
            resourceRequest.resourcePlan.metaTags[METATAGS_BIZINFO_COUNT_KEY] = std::to_string(scheduleInfo.count);
            resourceRequest.resourcePlan.metaTags[METATAGS_BIZINFO_MIN_AVAILABLE_COUNT_KEY] = std::to_string(scheduleInfo.minHealthCount);
        }
        resourceRequest.resourcePlan.metaTags["META_TAG_WORKDIR"] = roleId;
        map<tag_t, WorkerNodeVect>::const_iterator it = tagedWorkerNodes.find(tag);
        if (it != tagedWorkerNodes.end()) {
            for (size_t i = 0; i < it->second.size(); i++) {
                const WorkerNodePtr &workerNodePtr = it->second[i];
                resourceRequest.launchPlan = workerNodePtr->getFinalPlan().launchPlan;
                break;
            }
        }
        CARBON_LOG(DEBUG, "resource request, tag:%s, count:%d.",
                   tag.c_str(), count);
    }
}

int32_t RoleExecutor::countSlotsNeedAllocate(const tag_t &tag,
        const map<tag_t, WorkerNodeVect> &tagedWorkerNodes)
{
    map<tag_t, WorkerNodeVect>::const_iterator it =
        tagedWorkerNodes.find(tag);
    if (it == tagedWorkerNodes.end()) {
        return 0;
    }

    int32_t count = 0;
    const WorkerNodeVect &workerNodes = it->second;
    for (size_t i = 0; i < workerNodes.size(); i++) {
        const WorkerNodePtr &workerNodePtr = workerNodes[i];
        if (workerNodePtr->isNeedSlot()) {
            count ++;
        }
    }

    return count;
}

void RoleExecutor::getReleaseSlots(
        const map<tag_t, WorkerNodeVect> &tagedWorkerNodes,
        const SlotInfoMap &slotInfoMap,
        map<SlotId, ReleasePreference> *releaseSlots)
{
    map<tag_t, WorkerNodeVect>::const_iterator it = tagedWorkerNodes.begin();
    set<SlotId> assignedSlots;
    for (it = tagedWorkerNodes.begin(); it != tagedWorkerNodes.end(); it++) {
        const WorkerNodeVect &workerNodes = it->second;
        for (size_t i = 0; i < workerNodes.size(); i++) {
            const WorkerNodePtr workerNodePtr = workerNodes[i];
            SlotId slotId = workerNodePtr->getSlotId();
            assignedSlots.insert(slotId);
            if (workerNodePtr->canReleaseSlot()) {
                (*releaseSlots)[slotId] = workerNodePtr->getReleasePreference();
                CARBON_LOG(INFO, "add releasing slot: %s.",
                        ToJsonString(slotId, true).c_str());
            }
        }
    }
    for (auto slotIt = slotInfoMap.begin();
         slotIt != slotInfoMap.end(); slotIt++)
    {
        const SlotId &slotId = slotIt->first;
        if (assignedSlots.find(slotId) == assignedSlots.end()) {
            (*releaseSlots)[slotId] = ReleasePreference();
            CARBON_LOG(WARN, "add releasing slot extra: %s.",
                       ToJsonString(slotId, true).c_str());
        }
    }
}

void RoleExecutor::getLaunchPlans(const WorkerNodeVect &allWorkerNodes,
                                  map<SlotId, LaunchPlan> *curLaunchPlanMap,
                                  map<SlotId, LaunchPlan> *finalLaunchPlanMap)
{
    for (size_t i = 0; i < allWorkerNodes.size(); i++) {
        const WorkerNodePtr &workerNodePtr = allWorkerNodes[i];
        if (!workerNodePtr->isAssignedSlot())
        {
            continue;
        }
        const VersionedPlan &curPlan = workerNodePtr->getCurPlan();
        const VersionedPlan &finalPlan = workerNodePtr->getFinalPlan();
        SlotId slotId = workerNodePtr->getSlotId();
        const nodeid_t &nodeId = workerNodePtr->getId();
        (*curLaunchPlanMap)[slotId] = curPlan.launchPlan;
        (*finalLaunchPlanMap)[slotId] = finalPlan.launchPlan;
        fillRoleUidInLaunchPlan(nodeId, &((*curLaunchPlanMap)[slotId]));
    }
}

void RoleExecutor::fillRoleUidInLaunchPlan(const nodeid_t &nodeId, LaunchPlan *launchPlan) {
    vector<ProcessInfo> &processInfos = launchPlan->processInfos;
    for (size_t i = 0; i < processInfos.size(); i++) {
        vector<hippo::PairType> &envs = processInfos[i].envs;
        if (checkEnvExisted(envs, WORKER_IDENTIFIER_FOR_CARBON)) {
            CARBON_LOG(ERROR, "ENV VAR %s has existed for nodeId:[%s].",
                       WORKER_IDENTIFIER_FOR_CARBON.c_str(), nodeId.c_str());
            continue;
        }
        const string uniqIdentifier = GlobalVariable::genUniqIdentifier(nodeId);
        envs.push_back(make_pair(WORKER_IDENTIFIER_FOR_CARBON, uniqIdentifier));
    }
}

bool RoleExecutor::checkEnvExisted(const vector<hippo::PairType> &envs, const string &key) {
    for (size_t i = 0; i < envs.size(); i++) {
        if (envs[i].first == key) {
            CARBON_LOG(ERROR, "key [%s] has existed in envs. value is [%s].",
                       key.c_str(), envs[i].second.c_str());
            return true;
        }
    }
    return false;
}

END_CARBON_NAMESPACE(master);

