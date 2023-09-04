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
#include "sdk/default/DefaultProcessLauncher.h"
#include "sdk/default/ProcessStartWorkItem.h"
#include "hippo/ProtoWrapper.h"
#include "util/SignatureUtil.h"

using namespace std;
using namespace autil;

USE_HIPPO_NAMESPACE(util);
BEGIN_HIPPO_NAMESPACE(sdk);

HIPPO_LOG_SETUP(sdk, DefaultProcessLauncher);

DefaultProcessLauncher::DefaultProcessLauncher()
    : ProcessLauncherBase()
    , _processCheckInterval(30000000)
{
    auto charPtr = std::getenv("HOME");
    if (charPtr) {
        _homeDir = string(charPtr);
    }
}

DefaultProcessLauncher::~DefaultProcessLauncher() {
}

bool DefaultProcessLauncher::isPackageReady(const SlotInfo &slotInfo) const {
    return true;
}

bool DefaultProcessLauncher::isPreDeployPackageReady(const SlotInfo &slotInfo) const
{
    return true;
}

bool DefaultProcessLauncher::isDataReady(const SlotInfo &slotInfo,
        const vector<string> &dataNames) const
{
    return true;
}

void DefaultProcessLauncher::launch(const map<string, std::set<SlotId> > &slotIds)
{
    HIPPO_LOG(INFO, "begin launch");
    if (!_controller.started()) {
        _controller.start();
    }
    clearUselessContexts(slotIds);
    map<string, std::set<SlotId> > releasedSlotIds;
    getReleasedSlots(slotIds, releasedSlotIds);
    stopProcess(releasedSlotIds);
    _role2SlotIds = slotIds;
    asyncLaunch(slotIds);
    HIPPO_LOG(DEBUG, "after async launch, counter:%d", _counter.peek());
    _counter.wait();
    HIPPO_LOG(INFO, "end launch");
}

void DefaultProcessLauncher::asyncLaunch(const map<string, set<hippo::SlotId> > &slotIds) {
    for (map<string, set<hippo::SlotId> >::const_iterator it = slotIds.begin();
         it != slotIds.end(); ++it)
    {
        const string &role = it->first;
        HIPPO_LOG(DEBUG, "begin launch role:%s, slot count:%ld",
                  role.c_str(), it->second.size());
        asyncLaunchOneRole(role, it->second);
    }
}

void DefaultProcessLauncher::asyncLaunchOneRole(const string &role,
        const set<hippo::SlotId> &slotIds)
{
    for (set<hippo::SlotId>::const_iterator it = slotIds.begin();
         it != slotIds.end(); ++it)
    {
        const hippo::SlotId &slotId = *it;
        hippo::ProcessContext slotContext;
        HIPPO_LOG(DEBUG, "begin launch slot, slot address:%s, id:%d",
                  slotId.slaveAddress.c_str(), slotId.id);
        if (!getSlotProcessContext(role, slotId, &slotContext)) {
            HIPPO_LOG(WARN, "get slot process context failed, "
                      "role:%s, slot address:%s, id:%d",role.c_str(),
                      slotId.slaveAddress.c_str(), slotId.id);
            continue;
        }
        asyncLaunchOneSlot(role, slotId, slotContext);
    }
}

void DefaultProcessLauncher::asyncLaunchOneSlot(
        const string& role,
        const hippo::SlotId &slotId,
        const hippo::ProcessContext &slotProcessContext)
{
    HIPPO_LOG(DEBUG, "begin launch slot, role:%s, slot address:%s slot id:%d",
              role.c_str(), slotId.slaveAddress.c_str(), slotId.id);
    ProcessStartWorkItem *workItem = new ProcessStartWorkItem();
    workItem->applicationId  = _applicationId;
    workItem->role = role;
    workItem->slotId = slotId;
    workItem->homeDir = _homeDir;
    workItem->cmdExecutor = _controller.getCmdExecutor();
    getLaunchSignature(slotId, workItem->launchSignature);
    ProtoWrapper::convert(slotProcessContext, &(workItem->processContext));
    workItem->callback = std::bind(&DefaultProcessLauncher::callback, this,
                                   std::placeholders::_1, std::placeholders::_2,
                                   std::placeholders::_3, std::placeholders::_4);
    if (!getSlotResource(slotId, workItem->slotResource)) {
        HIPPO_LOG(WARN, "get slot resource failed, slot role:%s, "
                  "slot address:%s slot id:%d",
                  role.c_str(), slotId.slaveAddress.c_str(), slotId.id);
        return;
    }
    if (!needLaunch(slotId)) {
        HIPPO_LOG(DEBUG, "no need launch, slot role:%s, slot address:%s slot id:%d",
                  role.c_str(), slotId.slaveAddress.c_str(), slotId.id);
        return;
    }
    HIPPO_LOG(INFO, "start process for role:[%s], slaveAddr:[%s], slot:[%d], declareTime:%ld",
              role.c_str(), slotId.slaveAddress.c_str(), slotId.id, slotId.declareTime);
    HIPPO_LOG(DEBUG, "process context:[%s]", workItem->processContext.DebugString().c_str());
    _counter.inc();
    if (!_controller.startProcess(workItem)) {
        HIPPO_LOG(ERROR, "start process on slave:[%s] failed.",
                  slotId.slaveAddress.c_str());
        delete workItem;
        _counter.dec();
    }
}

bool DefaultProcessLauncher::getSlotResource(const hippo::SlotId &slotId,
        hippo::SlotResource &slotResource)
{
    auto it = _slotResources.find(slotId);
    if (it != _slotResources.end()) {
        slotResource = it->second;
        HIPPO_LOG(DEBUG, "get resource, resource size:%ld", slotResource.resources.size());
        for (auto &resource : slotResource.resources) {
            HIPPO_LOG(DEBUG, "resource name:%s resource value:%d",
                      resource.name.c_str(), resource.amount);
        }
        return true;
    }

    return false;
}

void DefaultProcessLauncher::getReleasedSlots(
        const map<string, set<hippo::SlotId> > &slotIds,
        map<string, set<hippo::SlotId> > &releasedSlots)
{
    for (const auto &roleSlots : _role2SlotIds) {
        auto it = slotIds.find(roleSlots.first);
        if (it == slotIds.end()) {
            releasedSlots[roleSlots.first] = roleSlots.second;
        } else {
            auto &newSlots = it->second;
            auto &oldSlots = roleSlots.second;
            for (auto &slotId : oldSlots) {
                if (newSlots.count(slotId) == 0) {
                    releasedSlots[roleSlots.first].insert(slotId);
                }
            }
        }
    }
}

void DefaultProcessLauncher::stopProcess(
        const map<string, set<hippo::SlotId> > &releasedSlots)
{
    for (const auto& roleSlotIds : releasedSlots) {
        for (const auto& slotId : roleSlotIds.second) {
            HIPPO_LOG(INFO, "stop unused slot, role:[%s], "
                      "slot address:[%s], slot id:[%d]", roleSlotIds.first.c_str(),
                      slotId.slaveAddress.c_str(), slotId.id);
            _controller.stopProcess(slotId);
        }
    }
}

bool DefaultProcessLauncher::resetSlot(const hippo::SlotId &slotId) {
    return _controller.resetSlot(slotId);
}

bool DefaultProcessLauncher::getLaunchSignature(const hippo::SlotId &slotId,
        int64_t &signature)
{
    map<hippo::SlotId, LaunchMeta>::const_iterator it = _launchMetas.find(slotId);
    if (it == _launchMetas.end()) {
        return false;
    }
    signature = it->second.launchSignature;
    return true;
}

bool DefaultProcessLauncher::needLaunch(const hippo::SlotId &slotId) {
    int64_t targetSignature = -1;
    if (!getLaunchSignature(slotId, targetSignature)) {
        HIPPO_LOG(DEBUG, "target launch meta not exists, no need launch, "
                  "slotId address:%s, slotId id:%d", slotId.slaveAddress.c_str(),
                  slotId.id);
        return false;
    }
    int64_t currentSignature = -1;
    ScopedLock lock(_mutex);
    auto cIt = _launchedMetas.find(slotId);
    if (cIt == _launchedMetas.end()) {
        HIPPO_LOG(DEBUG, "launched meta not exists, need launch, "
                  "slotId address:%s, slotId id:%d", slotId.slaveAddress.c_str(),
                  slotId.id);
            return true;
    }
    currentSignature = cIt->second.first;
    HIPPO_LOG(DEBUG, "launch meta, slotId address:%s, slotId id:%d, "
              "src launchSignature:%ld, target launchSignature:%ld",
              slotId.slaveAddress.c_str(), slotId.id,
              currentSignature, targetSignature);
    int64_t currentTime = TimeUtility::currentTime();
    if (targetSignature != currentSignature) {
        return true;
    } else if (currentTime - _processCheckInterval > cIt->second.second) {
        HIPPO_LOG(DEBUG, "need check slot status, slotId address:%s, slotId id:%d, "
                  "currentTime:%ld lastCheckTime:%ld",
                  slotId.slaveAddress.c_str(), slotId.id,
                  currentTime, cIt->second.second);
            return true;
    }
    return false;
}

void DefaultProcessLauncher::callback(const hippo::SlotId &slotId,
                                      int64_t launchSignature,
                                      ProcessError ret,
                                      const string& errorMsg)
{
    _counter.dec();
    if (ret != ERROR_NONE) {
        clearLaunchedSignature(slotId);
        HIPPO_LOG(ERROR, "start process failed, msg:%s", errorMsg.c_str());
        return;
    }
    setLaunchedSignature(slotId, launchSignature);
    HIPPO_LOG(DEBUG, "start process callback, counter:%d, signature:%d",
              _counter.peek(), launchSignature);
}

void DefaultProcessLauncher::clearLaunchedSignature(const hippo::SlotId &slotId)
{
    ScopedLock lock(_mutex);
    _launchedMetas.erase(slotId);
}

void DefaultProcessLauncher::setLaunchedSignature(const hippo::SlotId &slotId,
        int64_t launchSignature)
{
    int64_t currentTime = TimeUtility::currentTime();
    ScopedLock lock(_mutex);
    _launchedMetas[slotId].first = launchSignature;
    _launchedMetas[slotId].second = currentTime;
}

void DefaultProcessLauncher::setLaunchMetas(
        const std::map<hippo::SlotId, LaunchMeta> &launchMetas)
{
    _launchMetas = launchMetas;
    ScopedLock lock(_mutex);
    //clear useless slot
    for (auto it = _launchedMetas.begin(); it != _launchedMetas.end();) {
        if (launchMetas.find(it->first) == launchMetas.end()) {
            _launchedMetas.erase(it++);
        } else {
            it++;
        }
    }
}

std::map<hippo::SlotId, LaunchMeta> DefaultProcessLauncher::getLaunchedMetas() {
    map<hippo::SlotId, LaunchMeta> launchedMetaMap;
    ScopedLock lock(_mutex);
    for (const auto &it : _launchedMetas) {
        launchedMetaMap[it.first].launchSignature = it.second.first;
    }
    return launchedMetaMap;
}

void DefaultProcessLauncher::setApplicationId(const string &appId) {
    ProcessLauncherBase::setApplicationId(appId);
    _controller.setApplicationId(appId);
}

END_HIPPO_NAMESPACE(sdk);
