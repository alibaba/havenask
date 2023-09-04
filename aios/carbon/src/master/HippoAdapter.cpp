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
#include "master/HippoAdapter.h"
#include "carbon/Log.h"

using namespace std;
using namespace autil;
using namespace hippo;
BEGIN_CARBON_NAMESPACE(master);

CARBON_LOG_SETUP(master, HippoAdapter);

HippoAdapter::HippoAdapter() {
    _masterDriver = NULL;
}

HippoAdapter::~HippoAdapter() {
    delete _masterDriver;
}

bool HippoAdapter::init(const std::string &hippoZkRoot,
                        worker_framework::LeaderElector *leaderElector,
                        const std::string &applicationId)
{
    ScopedLock lock(_lock);
    if (_masterDriver != NULL) {
        CARBON_LOG(ERROR, "master driver is already created.");
        return false;
    }
    _masterDriver = MasterDriver::createDriver(HIPPO_DRIVER_NAME);
    if (!_masterDriver->init(hippoZkRoot, leaderElector, applicationId)) {
        CARBON_LOG(ERROR, "init master driver failed.");
        delete _masterDriver;
        _masterDriver = NULL;
        return false;
    }
    return true;
}

bool HippoAdapter::start() {
    ScopedLock lock(_lock);    
    if (!_masterDriver->start()) {
        CARBON_LOG(ERROR, "start master driver failed.");        
        delete _masterDriver;
        _masterDriver = NULL;        
        return false;
    }
    return true;
}

bool HippoAdapter::stop() {
    ScopedLock lock(_lock);
    if (!_masterDriver->stop()) {
        return false;
    }
    return true;
}

void HippoAdapter::fillRoleRequest(
        const ResourceRequest &resourceRequest,
        RoleRequest *roleRequest)
{
    roleRequest->options = resourceRequest.resourcePlan.resources;
    roleRequest->declare = resourceRequest.resourcePlan.declarations;
    roleRequest->allocateMode = resourceRequest.resourcePlan.allocateMode;
    roleRequest->queue = resourceRequest.resourcePlan.queue;
    roleRequest->priority = resourceRequest.resourcePlan.priority;
    roleRequest->groupId = resourceRequest.resourcePlan.group;
    roleRequest->metaTags = resourceRequest.resourcePlan.metaTags;
    roleRequest->count = resourceRequest.count;
    roleRequest->cpusetMode = resourceRequest.resourcePlan.cpusetMode;
    roleRequest->containerConfigs = resourceRequest.resourcePlan.containerConfigs;
    roleRequest->constraints = resourceRequest.resourcePlan.constraints;
    roleRequest->requirementId = resourceRequest.requirementId;
    ProcessContext context;
    fillProcessContext(resourceRequest.launchPlan, resourceRequest.launchPlan, &context);
    roleRequest->context = context;
}

void HippoAdapter::allocateSlots(
        const map<tag_t, ResourceRequest> &resourcePlans,
        const map<SlotId, ReleasePreference> &releasedSlots)
{
    ScopedLock lock(_lock);
    map<tag_t, ResourceRequest>::const_iterator it = resourcePlans.begin();
    for (; it != resourcePlans.end(); it++) {
        RoleRequest request;
        fillRoleRequest(it->second, &request);
        CARBON_LOG(DEBUG, "allocate count:[%d], request:[%s]",
                   it->second.count,
                   ToJsonString(it->second.resourcePlan, true).c_str());
        _masterDriver->updateRoleRequest(it->first, request);
    }
    map<SlotId, ReleasePreference>::const_iterator rIt = releasedSlots.begin();
    for (; rIt != releasedSlots.end(); rIt++) {
        CARBON_LOG(INFO, "release slot [%s:%d]", rIt->first.slaveAddress.c_str(), 
                rIt->first.id);
        _masterDriver->releaseSlot(rIt->first, rIt->second);
    }
}

void HippoAdapter::fillProcessContext(
        const LaunchPlan &curLaunchPlan,
        const LaunchPlan &finalLaunchPlan,
        ProcessContext *context)
{
    context->pkgs = curLaunchPlan.packageInfos;
    context->preDeployPkgs = finalLaunchPlan.packageInfos;
    context->processes = curLaunchPlan.processInfos;
    context->datas = curLaunchPlan.dataInfos;
}

void HippoAdapter::preUpdate() {
    map<SlotId, SlotInfo> slotInfos;
    getSlotInfos(&slotInfos);
    _allSlots.swap(slotInfos);
}

void HippoAdapter::launchSlots(const map<SlotId, LaunchPlan> &launchPlans,
                               const map<SlotId, LaunchPlan> &finalLaunchPlans)
{
    ScopedLock lock(_lock);
    for (const auto & kv : launchPlans) {
        SlotId slotId = kv.first;
        auto  it = _allSlots.find(slotId);
        if (it != _allSlots.end()) {
            vector<SlotInfo> slotInfoVec;
            slotInfoVec.push_back(it->second);
            const auto &launchPlan = kv.second;
            if (!launchPlan.podDesc.empty()) {
                _masterDriver->setSlotPodDesc(slotInfoVec, launchPlan.podDesc);
            } else {
                ProcessContext context;
                const auto &finalPlanItr = finalLaunchPlans.find(slotId);
                if (finalPlanItr != finalLaunchPlans.end()) {
                    const auto &finalLaunchPlan = finalPlanItr->second;
                    fillProcessContext(launchPlan, finalLaunchPlan, &context);
                } else {
                    fillProcessContext(launchPlan, launchPlan, &context);
                }
                _masterDriver->setSlotProcess(slotInfoVec, context);
            }
        } else {
            CARBON_LOG(WARN, "[%s:%d] not exist in masterDriver, can not launch it.",
                       slotId.slaveAddress.c_str(), slotId.id);
        }
    }
}

void HippoAdapter::extractSlotInfo(
        const SlotInfoVect &slotInfoVect,
        SlotInfoMap *slotInfos) const
{
    for (size_t i = 0; i < slotInfoVect.size(); i++) {
        const SlotInfo &slotInfo = slotInfoVect[i];
        (*slotInfos)[slotInfo.slotId] = slotInfo;
    }
}

void HippoAdapter::getSlotInfos(map<SlotId, SlotInfo> *slotInfos) {
    map<tag_t, SlotInfoVect> slots = getSlots();
    for (map<tag_t, SlotInfoVect>::iterator it = slots.begin();
         it != slots.end(); it++)
    {
        SlotInfoVect &slotInfoVect = it->second;
        extractSlotInfo(slotInfoVect, slotInfos);
    }
}

void HippoAdapter::releaseTag(const tag_t &tag) {
    ScopedLock lock(_lock);    
    ReleasePreference pref;
    CARBON_LOG(DEBUG, "release tag %s", tag.c_str());
    _masterDriver->releaseRoleSlots(tag, pref);
}

map<tag_t, SlotInfoVect> HippoAdapter::getSlots() const {
    ScopedLock lock(_lock);    
    map<tag_t, SlotInfoVect> slots;
    
    set<string> resourceTags;
    _masterDriver->getRoleNames(&resourceTags);
    
    set<string>::const_iterator it = resourceTags.begin();
    for (; it != resourceTags.end(); it++) {
        SlotInfoVect slotInfoVect;
        _masterDriver->getSlotsByRole(*it, &slotInfoVect);
        slots[*it] = slotInfoVect;
    }
    return slots;
}

// not used ?
SlotInfoMap HippoAdapter::getSlotsByTag(const tag_t &tag) const {
    ScopedLock lock(_lock);    
    SlotInfoVect slotInfoVect;
    _masterDriver->getSlotsByRole(tag, &slotInfoVect);
    SlotInfoMap res;
    extractSlotInfo(slotInfoVect, &res);
    return res;
}

SlotInfoMap HippoAdapter::getSlotsByTags(const std::set<tag_t> &tags) const {
    ScopedLock lock(_lock);    
    SlotInfoMap res;
    set<tag_t>::const_iterator it = tags.begin();
    for (; it != tags.end(); it++) {
        SlotInfoVect slotInfoVect;
        _masterDriver->getSlotsByRole(*it, &slotInfoVect);
        extractSlotInfo(slotInfoVect, &res);
    }
    return res;
}

void HippoAdapter::getAllTags(set<tag_t> *tags) const {
    ScopedLock lock(_lock);
    _masterDriver->getRoleNames(tags);
    vector<SlotInfo> slotInfos;
    _masterDriver->getSlots(&slotInfos);
    for (size_t i = 0; i < slotInfos.size(); i++) {
        tags->insert(slotInfos[i].role);
    }
}

bool HippoAdapter::isWorking() const {
    ScopedLock lock(_lock);    
    return _masterDriver->isWorking();
}

int64_t HippoAdapter::getAppChecksum() const {
    ScopedLock lock(_lock);
    return _masterDriver->getAppChecksum();
}
END_CARBON_NAMESPACE(master);
