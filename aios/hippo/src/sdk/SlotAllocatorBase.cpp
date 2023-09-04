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
#include "sdk/SlotAllocatorBase.h"
#include "hippo/ProtoWrapper.h"
#include "util/SignatureUtil.h"
#include "util/PackageUtil.h"
#include "hippo/HippoUtil.h"

using namespace std;
using namespace autil;
using namespace hippo;
USE_HIPPO_NAMESPACE(util);
BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, SlotAllocatorBase);

SlotAllocatorBase::SlotAllocatorBase(EventTrigger *eventTrigger)
    : _eventTrigger(eventTrigger)
{
}

SlotAllocatorBase::~SlotAllocatorBase() {
    clearRoleSlotInfoMap(&_allocatedSlots);
}

void SlotAllocatorBase::updateResourceRequest(
        const string &role,
        const RoleRequest &request)
{
    ScopedLock lock(_mutex);
    _resourceRequests[role] = request;
    if (request.count > 0) {
        // stop release this role
        _needReleaseRoles.erase(role);
    }
}

void SlotAllocatorBase::releaseSlot(const SlotId &slotId,
                                    const ReleasePreference &pref,
                                    int32_t slotReserveTime)
{
    ScopedLock lock(_mutex);
    _needReleaseSlots[slotId] = ReleaseOptions(pref, slotReserveTime);
}

void SlotAllocatorBase::releaseRoleSlots(const string &role,
        const ReleasePreference &pref,
        int32_t roleSlotReserveTime)
{
    ScopedLock lock(_mutex);
    _needReleaseRoles[role] = ReleaseOptions(pref, roleSlotReserveTime);
    // stop allocate slots for this role
    HIPPO_LOG(INFO, "release role slots:[%s]", role.c_str());
    RoleResourceRequestMap::iterator it = _resourceRequests.find(role);
    if (it != _resourceRequests.end()) {
        it->second.count = 0;
    }
}

void SlotAllocatorBase::getSlots(vector<SlotInfo> *slots) const {
    if (!slots) {
        return;
    }
    ScopedLock lock(_mutex);
    for (RoleSlotInfoMap::const_iterator it = _allocatedSlots.begin();
         it != _allocatedSlots.end(); ++it)
    {
        const SlotInfoMap &slotInfos = it->second;
        for (SlotInfoMap::const_iterator it2 = slotInfos.begin();
             it2 != slotInfos.end(); ++it2)
        {
            const hippo::SlotId &slotId = it2->first;
            const SlotInfo *slotInfo = it2->second;
            if (!needRelease(slotInfo->role, slotId)) {
                slots->push_back(*slotInfo);
            }
        }
    }
}

void SlotAllocatorBase::getSlotsByRole(const string &role,
                                       vector<SlotInfo> *slots) const
{
    if (!slots) {
        return;
    }
    ScopedLock lock(_mutex);
    RoleSlotInfoMap::const_iterator it = _allocatedSlots.find(role);
    if (it == _allocatedSlots.end()) {
        return;
    }
    const SlotInfoMap &slotInfos = it->second;
    for (SlotInfoMap::const_iterator it2 = slotInfos.begin();
         it2 != slotInfos.end(); ++it2)
    {
        const hippo::SlotId &slotId = it2->first;
        const SlotInfo *slotInfo = it2->second;
        if (!needRelease(slotInfo->role, slotId)) {
            slots->push_back(*slotInfo);
        }
    }
}

void SlotAllocatorBase::getReclaimingSlots(vector<SlotInfo> *slots) const
{
    if (!slots) {
        return;
    }
    ScopedLock lock(_mutex);
    for (RoleSlotInfoMap::const_iterator it = _allocatedSlots.begin();
         it != _allocatedSlots.end(); ++it)
    {
        const SlotInfoMap &slotInfos = it->second;
        for (SlotInfoMap::const_iterator it2 = slotInfos.begin();
             it2 != slotInfos.end(); ++it2)
        {
            const SlotInfo *slotInfo = it2->second;
            if (slotInfo->reclaiming) {
                slots->push_back(*slotInfo);
            }
        }
    }
}

bool SlotAllocatorBase::getSlotInfo(const SlotId &slotId,
                                    SlotInfo *slotInfo) const
{
    assert(slotInfo);
    ScopedLock lock(_mutex);
    for (RoleSlotInfoMap::const_iterator it = _allocatedSlots.begin();
         it != _allocatedSlots.end(); ++it)
    {
        const SlotInfoMap &slotInfos = it->second;
        SlotInfoMap::const_iterator it2 = slotInfos.find(slotId);
        if (it2 != slotInfos.end()) {
            *slotInfo = *(it2->second);
            return true;
        }
    }
    return false;
}

bool SlotAllocatorBase::allocate() {
    proto::AllocateRequest request;
    proto::AllocateResponse response;

    createAllocateRequest(&request);

    if (doAllocate(request, &response)) {
        processResponse(response);
        return true;
    } else {
        return false;
    }
}

void SlotAllocatorBase::createAllocateRequest(
        proto::AllocateRequest *request) const
{
    ScopedLock lock(_mutex);
    request->set_applicationid(_applicationId);
    fillResourceRequest(request);
    fillReleaseSlots(request);
}

void SlotAllocatorBase::fillResourceRequest(proto::AllocateRequest *request) const {
    for (RoleResourceRequestMap::const_iterator it = _resourceRequests.begin();
         it != _resourceRequests.end(); it++)
    {
        proto::ResourceRequest *resourceRequest = request->add_require();
        // property
        const RoleRequest &roleRequest = it->second;
        if (!roleRequest.requirementId.empty()) {
            resourceRequest->set_requirementid(roleRequest.requirementId);
        }
        resourceRequest->set_tag(it->first);
        resourceRequest->set_count(roleRequest.count);
        resourceRequest->set_queue(roleRequest.queue);
        resourceRequest->set_groupid(roleRequest.groupId);
        if (roleRequest.priority.majorPriority != Priority::UNDEFINE) {
            proto::Priority *priority = resourceRequest->mutable_priority();
            priority->set_majorpriority(roleRequest.priority.majorPriority);
            priority->set_minorpriority(roleRequest.priority.minorPriority);
        }
        // for protect
        if (roleRequest.cpusetMode == CpusetMode::UNDEFINE) {
            resourceRequest->set_cpusetmode(proto::ResourceRequest::RESERVED);
        } else {
            resourceRequest->set_cpusetmode(
                    (proto::ResourceRequest::CpusetMode)roleRequest.cpusetMode);
        }
        resourceRequest->set_allocatemode(
                (proto::ResourceRequest::AllocateMode)roleRequest.allocateMode);
        for (map<string, string>::const_iterator mtIt = roleRequest.metaTags.begin();
             mtIt != roleRequest.metaTags.end(); mtIt++)
        {
            proto::Parameter *metaTag = resourceRequest->add_metatags();
            metaTag->set_key(mtIt->first);
            metaTag->set_value(mtIt->second);
        }

        // resources
        const vector<SlotResource> &options = roleRequest.options;
        for (size_t i = 0; i < options.size(); i++) {
            const hippo::SlotResource &option = options[i];
            proto::SlotResource *slotResource = resourceRequest->add_options();
            for (size_t j = 0; j < option.resources.size(); j++) {
                proto::Resource *resource = slotResource->add_resources();
                const hippo::Resource &optionResource = option.resources[j];
                resource->set_name(optionResource.name);
                resource->set_amount(optionResource.amount);
                resource->set_type((proto::Resource_Type)optionResource.type);
            }
        }
        const vector<Resource> &declare = roleRequest.declare;
        for (size_t i = 0; i < declare.size() ; i++) {
            const hippo::Resource &resource = declare[i];
            proto::Resource *declaration = resourceRequest->add_declarations();
            declaration->set_name(resource.name);
            declaration->set_amount(resource.amount);
            declaration->set_type((proto::Resource_Type)resource.type);
        }
        // constraints
        const ConstraintConfig &constraints = roleRequest.constraints;
        resourceRequest->set_spreadlevel(
                (proto::ResourceRequest_SpreadLevel)constraints.level);
        resourceRequest->set_strictly(constraints.strictly);
        resourceRequest->set_usehostworkdir(constraints.useHostWorkDir);
#define CHECK_AND_SET(maxinstanceperXX, maxInstancePerXX)               \
        if (constraints.maxInstancePerXX > 0) {                         \
            resourceRequest->set_##maxinstanceperXX(constraints.maxInstancePerXX); \
        }
        CHECK_AND_SET(maxinstanceperhost, maxInstancePerHost);
        CHECK_AND_SET(maxinstanceperframe, maxInstancePerFrame);
        CHECK_AND_SET(maxinstanceperrack, maxInstancePerRack);
        CHECK_AND_SET(maxinstanceperasw, maxInstancePerASW);
        CHECK_AND_SET(maxinstanceperpsw, maxInstancePerPSW);
#undef CHECK_AND_SET

        for (size_t i = 0 ;i < constraints.specifiedIps.size(); ++i) {
            *(resourceRequest->add_specifiedips()) = constraints.specifiedIps[i];
        }
        for (size_t i = 0 ;i < constraints.prohibitedIps.size(); ++i) {
            *(resourceRequest->add_prohibitedips()) = constraints.prohibitedIps[i];
        }
        for (size_t i = 0; i < roleRequest.containerConfigs.size(); ++i) {
            *resourceRequest->add_containerconfigs() =
                roleRequest.containerConfigs[i];
        }
        if (!roleRequest.context.processes.empty()) {
            proto::ProcessLaunchContext* launchtemplate = resourceRequest->mutable_launchtemplate();
            ProtoWrapper::convert(roleRequest.context, launchtemplate);
            auto signature = SignatureUtil::signature(*launchtemplate);
            resourceRequest->set_launchsignature(signature);
            resourceRequest->set_packagechecksum(hippo::HippoUtil::genPackageChecksum(roleRequest.context.pkgs));
        }
    }
}

void SlotAllocatorBase::fillReleaseSlots(proto::AllocateRequest *request) const {
    for (RoleSlotInfoMap::const_iterator it = _allocatedSlots.begin();
         it != _allocatedSlots.end(); ++it)
    {
        const RoleName &role = it->first;
        const SlotInfoMap &slotInfos = it->second;
        for (SlotInfoMap::const_iterator it2 = slotInfos.begin();
             it2 != slotInfos.end(); ++it2)
        {
            const SlotId &slotId = it2->first;
            ReleaseOptions releaseOpts;
            if (needRelease(role, slotId, &releaseOpts)) {
                proto::ReserveSlot *resSlot = request->add_reserveslot();
                resSlot->set_reservetime(releaseOpts.reserveTime);
                proto::SlotId *slotIdProto = resSlot->mutable_slotid();
                slotIdProto->set_slaveaddress(slotId.slaveAddress);
                slotIdProto->set_id(slotId.id);
                slotIdProto->set_declaretime(slotId.declareTime);
                slotIdProto->set_k8snamespace(slotId.k8sNamespace);
                slotIdProto->set_k8spodname(slotId.k8sPodName);
                slotIdProto->set_k8spoduid(slotId.k8sPodUID);
                proto::PreferenceDescription *prefDesc= request->add_preferencedesc();
                prefDesc->set_type((proto::PreferenceDescription::PreferenceType)releaseOpts.preference.type);
                prefDesc->set_leasems(releaseOpts.preference.leaseMs);
                prefDesc->set_slaveaddress(slotId.slaveAddress);
                prefDesc->set_resourcetag(role);
                if (releaseOpts.preference.workDirTag != "") {
                    prefDesc->set_workdirtag(releaseOpts.preference.workDirTag);
                }
            }
        }
    }
}

bool SlotAllocatorBase::needRelease(const RoleName &role,
                                const hippo::SlotId &slotId,
                                ReleaseOptions *releaseOpts) const
{
    map<RoleName, ReleaseOptions>::const_iterator releaseRoleIt =
        _needReleaseRoles.find(role);
    map<hippo::SlotId, ReleaseOptions>::const_iterator releaseSlotIt =
        _needReleaseSlots.find(slotId);
    if (releaseRoleIt != _needReleaseRoles.end()) {
        HIPPO_LOG(DEBUG, "slot release for role already released, role:%s "
                  "solt address:%s, slot id:%d, declareTime:%ld", role.c_str(),
                  slotId.slaveAddress.c_str(), slotId.id, slotId.declareTime);

        if (releaseOpts) {
            *releaseOpts = releaseRoleIt->second;
        }
        return true;
    }
    if (releaseSlotIt != _needReleaseSlots.end()) {
        HIPPO_LOG(DEBUG, "slot released, role:%s solt address:%s, "
                  "slot id:%d, declareTime:%ld", role.c_str(),
                  slotId.slaveAddress.c_str(), slotId.id, slotId.declareTime);
        if (releaseOpts) {
            *releaseOpts = releaseSlotIt->second;
        }
        return true;
    }
    return false;
}

void SlotAllocatorBase::processResponse(
        const proto::AllocateResponse &response)
{
    // TODO: here should deal error info, by luren.ygd
    if (response.errorinfo().errorcode() != proto::ERROR_NONE) {
        HIPPO_LOG(ERROR, "allocate failed. errorCode:[%d], errorMsg:[%s]",
                  response.errorinfo().errorcode(),
                  response.errorinfo().errormsg().c_str());
    }
    ProtoWrapper::convert(response.errorinfo(), &_lastErrorInfo);
    RoleSlotInfoMap newAllocatedSlots;
    generateAllocatedSlots(response, &newAllocatedSlots);
    ScopedLock lock(_mutex);
    triggerSlotEvents(_allocatedSlots, newAllocatedSlots);
    clearReleasedSlots(newAllocatedSlots);
    clearRoleSlotInfoMap(&_allocatedSlots);
    _allocatedSlots.swap(newAllocatedSlots);
}

void SlotAllocatorBase::generateAllocatedSlots(
        const proto::AllocateResponse &response,
        RoleSlotInfoMap *newAllocatedSlots) const
{
    for (int i = 0; i < response.assignedresources_size(); i++) {
        const proto::ResourceResponse &resourceResponse =
            response.assignedresources(i);
        const string &role = resourceResponse.resourcetag();
        if (role == INTERNAL_APPMASTER_RESOURCE_TAG) {
            continue;
        }
        SlotInfos newSlotInfos;
        (*newAllocatedSlots)[role] = SlotInfoMap();
        ProtoWrapper::convert(resourceResponse.assignedslots(), &newSlotInfos);
        for (size_t j = 0; j < newSlotInfos.size(); ++j) {
            SlotInfo *slotInfo = newSlotInfos[j];
            slotInfo->role = role;
            (*newAllocatedSlots)[role][slotInfo->slotId] = slotInfo;
        }
    }
}

void SlotAllocatorBase::triggerSlotEvents(
        const RoleSlotInfoMap &lastAllocatedSlots,
        const RoleSlotInfoMap &newAllocatedSlots) const
{
    SlotInfos newAllocSlots;
    SlotInfos reclaimingSlots;
    SlotInfos reclaimedSlots;
    SlotInfos resourceChangeSlots;
    SlotInfos statusChangeSlots;

    RoleSlotInfoMap::const_iterator newIt = newAllocatedSlots.begin();
    for (; newIt != newAllocatedSlots.end(); ++newIt) {
        const RoleName &role = newIt->first;
        const SlotInfoMap &newSlotInfos = newIt->second;
        SlotInfoMap oldSlotInfos;
        RoleSlotInfoMap::const_iterator lastIt = lastAllocatedSlots.find(role);
        if (lastIt != lastAllocatedSlots.end()) {
            oldSlotInfos = lastIt->second;
        }
        diffSlotInfos(oldSlotInfos, newSlotInfos,
                      &newAllocSlots, &reclaimingSlots, &reclaimedSlots,
                      &resourceChangeSlots, &statusChangeSlots);
    }

    RoleSlotInfoMap::const_iterator lastIt = lastAllocatedSlots.begin();
    for (; lastIt != lastAllocatedSlots.end(); ++lastIt) {
        const RoleName &role = lastIt->first;
        if (newAllocatedSlots.find(role) == newAllocatedSlots.end()) {
            // whole role disappeared
            const SlotInfoMap &reclaimedSlotInfos = lastIt->second;
            for (SlotInfoMap::const_iterator it = reclaimedSlotInfos.begin();
                 it != reclaimedSlotInfos.end(); ++it)
            {
                reclaimedSlots.push_back(it->second);
            }
        }
    }

    trigger(EVENT_RESOURCE_NEW_ALLOC, newAllocSlots, "new allocate slots");
    trigger(EVENT_RESOURCE_RECLAIMING, reclaimingSlots, "reclaiming slots");
    trigger(EVENT_RESOURCE_RECLAIMED, reclaimedSlots, "reclaimed slots");
    trigger(EVENT_SLOT_RESOURCE_CHANGE, resourceChangeSlots, "resource change slots");
    trigger(EVENT_SLOT_STATUS_CHANGE, statusChangeSlots, "status change slots");
}

void SlotAllocatorBase::trigger(hippo::MasterDriverEvent events,
                            const SlotInfos &slotInfos, string message) const
{
    if (slotInfos.size() == 0) {
        return;
    }
    (*_eventTrigger)(events, &slotInfos, message);
}

void SlotAllocatorBase::diffSlotInfos(const SlotInfoMap &oldSlotInfos,
                                  const SlotInfoMap &newSlotInfos,
                                  SlotInfos *newAllocSlots,
                                  SlotInfos *reclaimingSlots,
                                  SlotInfos *reclaimedSlots,
                                  SlotInfos *resourceChangeSlots,
                                  SlotInfos *statusChangeSlots) const
{
    SlotInfoMap::const_iterator oldIt = oldSlotInfos.begin();
    SlotInfoMap::const_iterator newIt = newSlotInfos.begin();
    while (oldIt != oldSlotInfos.end() && newIt != newSlotInfos.end()) {
        if (oldIt->first < newIt->first) {
            HIPPO_LOG(DEBUG, "reclaimed slot:[%d]", oldIt->first.id);
            reclaimedSlots->push_back(oldIt->second);
            oldIt++;
        } else if (newIt->first < oldIt->first) {
            HIPPO_LOG(DEBUG, "new alloc slot:[%d]", newIt->first.id);
            newAllocSlots->push_back(newIt->second);
            if (newIt->second->reclaiming) {
                reclaimingSlots->push_back(newIt->second);
            }
            newIt++;
        } else {
            diffSlotInfo(oldIt->second, newIt->second, reclaimingSlots,
                         resourceChangeSlots, statusChangeSlots);
            oldIt++;
            newIt++;
        }
    }
    while (oldIt != oldSlotInfos.end()) {
        HIPPO_LOG(DEBUG, "reclaimed slot:[%d]", oldIt->first.id);
        reclaimedSlots->push_back(oldIt->second);
        oldIt++;
    }
    while (newIt != newSlotInfos.end()) {
        HIPPO_LOG(DEBUG, "new alloc slot:[%d]", newIt->first.id);
        newAllocSlots->push_back(newIt->second);
        if (newIt->second->reclaiming) {
            reclaimingSlots->push_back(newIt->second);
        }
        newIt++;
    }
}

void SlotAllocatorBase::diffSlotInfo(hippo::SlotInfo *oldSlotInfo,
                                 hippo::SlotInfo *newSlotInfo,
                                 SlotInfos *reclaimingSlots,
                                 SlotInfos *resourceChangeSlots,
                                 SlotInfos *statusChangeSlots) const
{
    if (oldSlotInfo->reclaiming == false && newSlotInfo->reclaiming == true) {
        HIPPO_LOG(DEBUG, "reclaiming slot:[%d]", newSlotInfo->slotId.id);
        reclaimingSlots->push_back(newSlotInfo);
    }
    if (slotStatusChange(oldSlotInfo, newSlotInfo)) {
        HIPPO_LOG(DEBUG, "status change slot:[%d]", newSlotInfo->slotId.id);
        statusChangeSlots->push_back(newSlotInfo);
    }
    if (slotResourceChange(oldSlotInfo, newSlotInfo)) {
        HIPPO_LOG(DEBUG, "resource change slot:[%d]", newSlotInfo->slotId.id);
        resourceChangeSlots->push_back(newSlotInfo);
    }
}

bool SlotAllocatorBase::slotStatusChange(const hippo::SlotInfo *oldSlotInfo,
                                     const hippo::SlotInfo *newSlotInfo) const
{
    if (oldSlotInfo->slaveStatus.status != newSlotInfo->slaveStatus.status) {
        return true;
    }
    return processStatusChange(oldSlotInfo, newSlotInfo);
}

bool SlotAllocatorBase::processStatusChange(const hippo::SlotInfo *oldSlotInfo,
                                        const hippo::SlotInfo *newSlotInfo) const
{
    if (oldSlotInfo->processStatus.size() != newSlotInfo->processStatus.size()) {
        return true;
    }

    map<string, hippo::ProcessStatus::Status> oldProcessStatusMap;
    for (size_t i = 0; i < oldSlotInfo->processStatus.size(); i++) {
        oldProcessStatusMap[oldSlotInfo->processStatus[i].processName] =
            oldSlotInfo->processStatus[i].status;
    }

    for (size_t i = 0; i < newSlotInfo->processStatus.size(); i++) {
        const string &procName = newSlotInfo->processStatus[i].processName;
        map<string, hippo::ProcessStatus::Status>::iterator it =
            oldProcessStatusMap.find(procName);
        if (it == oldProcessStatusMap.end()) {
            return true;
        }
        if (it->second != newSlotInfo->processStatus[i].status) {
            return true;
        }
    }
    return false;
}

bool SlotAllocatorBase::slotResourceChange(const hippo::SlotInfo *oldSlotInfo,
                                       const hippo::SlotInfo *newSlotInfo) const
{
    if (oldSlotInfo->slotResource.resources.size()
        != newSlotInfo->slotResource.resources.size())
    {
        return true;
    }

    map<string, hippo::Resource> oldResourceMap;
    for (size_t i = 0; i < oldSlotInfo->slotResource.resources.size(); i++) {
        oldResourceMap[oldSlotInfo->slotResource.resources[i].name] =
            oldSlotInfo->slotResource.resources[i];
    }

    for (size_t i = 0; i < newSlotInfo->slotResource.resources.size(); i++) {
        const hippo::Resource &newResource =
            newSlotInfo->slotResource.resources[i];
        map<string, hippo::Resource>::iterator it =
            oldResourceMap.find(newResource.name);
        if (it == oldResourceMap.end()) {
            return true;
        }
        if (it->second.type != newResource.type
            || it->second.amount != newResource.amount)
        {
            return true;
        }
    }
    return false;
}

void SlotAllocatorBase::clearRoleSlotInfoMap(RoleSlotInfoMap *roleSlotInfosMap) const {
    for (RoleSlotInfoMap::iterator it = roleSlotInfosMap->begin();
         it != roleSlotInfosMap->end(); it++)
    {
        const SlotInfoMap &slotInfos = it->second;
        for (SlotInfoMap::const_iterator it2 = slotInfos.begin();
             it2 != slotInfos.end(); ++it2)
        {
            delete it2->second;
        }
    }
    roleSlotInfosMap->clear();
}

void SlotAllocatorBase::clearReleasedSlots(const RoleSlotInfoMap &allocatedSlots) {
    set<hippo::SlotId> allSlots;
    for (RoleSlotInfoMap::const_iterator it = allocatedSlots.begin();
         it != allocatedSlots.end(); ++it)
    {
        const SlotInfoMap &slotInfos = it->second;
        for (SlotInfoMap::const_iterator it2 = slotInfos.begin();
             it2 != slotInfos.end(); ++it2)
        {
            allSlots.insert(it2->first);
        }
    }
    for (map<hippo::SlotId, ReleaseOptions>::iterator it =
             _needReleaseSlots.begin();
         it != _needReleaseSlots.end(); )
    {
        if (allSlots.count(it->first) == 0) {
            _needReleaseSlots.erase(it++);
        } else {
            it++;
        }
    }
    for (map<RoleName, ReleaseOptions>::iterator it =
             _needReleaseRoles.begin(); it != _needReleaseRoles.end(); )
    {
        if (allocatedSlots.find(it->first) == allocatedSlots.end()) {
            _resourceRequests.erase(it->first);
            _needReleaseRoles.erase(it++);
        } else {
            it++;
        }
    }
}

void SlotAllocatorBase::setApplicationId(const std::string &appId) {
    _applicationId = appId;
}

void SlotAllocatorBase::getRoleNames(std::set<std::string> *roleNames) const {
    RoleResourceRequestMap::const_iterator it =  _resourceRequests.begin();
    for (; it != _resourceRequests.end(); it++) {
        roleNames->insert(it->first);
    }
}

END_HIPPO_NAMESPACE(sdk);
