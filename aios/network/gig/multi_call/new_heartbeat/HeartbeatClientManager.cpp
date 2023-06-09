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
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatClientManager.h"
#include "aios/network/gig/multi_call/service/SearchServiceManager.h"
#include "aios/network/gig/multi_call/util/FileRecorder.h"

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatClientManager);

void HeartbeatClientManagerNotifier::notifyHeartbeatSuccess() const {
    autil::ScopedLock lock(_managerLock);
    if (_manager) {
        _manager->notifyHeartbeatSuccess();
    }
}

void HeartbeatClientManagerNotifier::stealManager() {
    autil::ScopedLock lock(_managerLock);
    _manager = nullptr;
}

HeartbeatClientManager::HeartbeatClientManager(SearchService *searchService,
                                               SearchServiceManager *manager)
    : _searchService(searchService)
    , _manager(manager)
    , _notifier(std::make_shared<HeartbeatClientManagerNotifier>(this))
{
}

HeartbeatClientManager::~HeartbeatClientManager() {
    _notifier->stealManager();
}

bool HeartbeatClientManager::start(
    const MiscConfigPtr &miscConfig,
    const std::shared_ptr<SearchServiceSnapshot> &heartbeatSnapshot)
{
    _miscConfig = miscConfig;
    _heartbeatSnapshot = heartbeatSnapshot;
    return initThread();
}

void HeartbeatClientManager::stop() {
    _notifier->stealManager();
    _thread.reset();
    auto hostMap = getHostMap();
    if (hostMap) {
        for (const auto &pair : *hostMap) {
            pair.second->stop();
        }
        setHostMap(nullptr);
    }
}

bool HeartbeatClientManager::initThread() {
    auto thread = autil::LoopThread::createLoopThread(
        std::bind(&HeartbeatClientManager::loop, this), 2 * 1000 * 1000, "GigClientHB");
    if (!thread) {
        AUTIL_LOG(ERROR, "create gig client heartbeat thread failed");
        return false;
    }
    _thread = thread;
    return true;
}

bool HeartbeatClientManager::updateSpecVec(const HeartbeatSpecVec &heartbeatSpecs) {
    bool hasDiff = false;
    auto newHostMapPtr = std::make_shared<HostHeartbeatInfoMap>();
    auto &newHostMap = *newHostMapPtr;
    auto oldHostMap = getHostMap();
    for (const auto &spec : heartbeatSpecs) {
        auto addr = spec.spec.getAnetSpec(MC_PROTOCOL_GRPC_STREAM);
        if (oldHostMap) {
            auto it = oldHostMap->find(addr);
            if (oldHostMap->end() != it) {
                newHostMap.emplace(*it);
                continue;
            }
        }
        if (newHostMap.end() != newHostMap.find(addr)) {
            continue;
        }
        auto info = std::make_shared<HostHeartbeatInfo>();
        info->init(_notifier, _heartbeatSnapshot, _searchService, spec);
        newHostMap.emplace(addr, info);
        hasDiff = true;
    }
    if (!hasDiff) {
        if (oldHostMap) {
            if (oldHostMap->size() != newHostMap.size()) {
                hasDiff = true;
            }
        }
    }
    setHostMap(newHostMapPtr);
    if (hasDiff) {
        if (_thread) {
            _thread->runOnce();
        }
        _notifier->notify();
    }
    return true;
}

void HeartbeatClientManager::notifyHeartbeatSuccess() {
    auto hostMap = getHostMap();
    if (!hostMap) {
        return;
    }
    bool notify = true;
    for (const auto &pair : *hostMap) {
        if (!pair.second->isTopoReady()) {
            notify = false;
            break;
        }
    }
    if (notify && _manager) {
        _manager->notifyUpdate();
    }
}

void HeartbeatClientManager::loop() {
    auto hostMap = getHostMap();
    if (!hostMap) {
        return;
    }
    for (const auto &pair : *hostMap) {
        pair.second->tick();
    }
}

bool HeartbeatClientManager::fillBizInfoMap(BizInfoMap &bizInfoMap) {
    auto hostMap = getHostMap();
    if (!hostMap) {
        return true;
    }
    for (const auto &pair : *hostMap) {
        const auto &hostInfo = pair.second;
        if (!hostInfo->fillBizInfoMap(bizInfoMap)) {
            return false;
        }
    }
    return true;
}

void HeartbeatClientManager::setHostMap(const HostHeartbeatInfoMapPtr &newMap) {
    autil::ScopedLock lock(_hostMapLock);
    _hostMap = newMap;
}

HostHeartbeatInfoMapPtr HeartbeatClientManager::getHostMap() const {
    autil::ScopedLock lock(_hostMapLock);
    return _hostMap;
}

void HeartbeatClientManager::log() {
    auto newSnapshotStr = toString();
    if (newSnapshotStr != _snapshotStr) {
        FileRecorder::recordSnapshot(newSnapshotStr, _miscConfig->snapshotLogCount,
                                     "gig_snapshot/heartbeat_client/" + _miscConfig->logPrefix);
        _snapshotStr.swap(newSnapshotStr);
    }
}

void HeartbeatClientManager::getSpecSet(std::set<std::string> &specSet) const {
    auto hostMapPtr = getHostMap();
    if (!hostMapPtr) {
        return;
    }
    const auto &hostMap = *hostMapPtr;
    for (const auto &pair : hostMap) {
        specSet.insert(pair.first);
    }
}

std::string HeartbeatClientManager::toString() const {
    auto hostMapPtr = getHostMap();
    if (!hostMapPtr) {
        return "";
    }
    const auto &hostMap = *hostMapPtr;
    std::string ret;
    ret += "hostCount: " + autil::StringUtil::toString(hostMap.size());
    ret += "\n";
    MetasSignatureMap allMetas;
    for (const auto &pair : hostMap) {
        pair.second->toString(ret, allMetas);
        ret += "\n";
    }
    ret += "metasCount: " + autil::StringUtil::toString(allMetas.size());
    ret += "\n";
    for (const auto &pair : allMetas) {
        ret += "  sig: " + autil::StringUtil::toString(pair.first);
        ret += "\n";
        const auto &metaMapPtr = pair.second;
        if (!metaMapPtr || metaMapPtr->empty()) {
            ret += "    (empty)\n";
            continue;
        }
        for (const auto &metaPair : *metaMapPtr) {
            ret += "    ";
            ret += autil::StringUtil::toString(metaPair.first);
            ret += " " + autil::StringUtil::toString(metaPair.second);
            ret += "\n";
        }
        ret += "\n";
    }
    return ret;
}

}
