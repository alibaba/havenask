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
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatServerManager.h"

#include "aios/network/gig/multi_call/new_heartbeat/BizTopo.h"
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatServerStreamQueue.h"
#include "aios/network/gig/multi_call/util/FileRecorder.h"
#include "aios/network/gig/multi_call/util/RandomGenerator.h"

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatServerManager);

HeartbeatServerManager::HeartbeatServerManager() : _serverTopoMap(new ServerTopoMap()) {
    atomic_set(&_idCounter, 0);
    atomic_set(&_nextQueueId, 0);
}

HeartbeatServerManager::~HeartbeatServerManager() {
    _logThread.reset();
    _streamQueues.clear();
}

bool HeartbeatServerManager::init(const GigAgentPtr &agent, const Spec &spec) {
    _agent = agent;
    _serverTopoMap->setSpec(spec);
    RandomGenerator rand(0);
    rand.get();
    atomic_set(&_idCounter, rand.get());
    for (size_t i = 0; i < QUEUE_COUNT; i++) {
        HeartbeatServerStreamQueuePtr queue = std::make_shared<HeartbeatServerStreamQueue>(i);
        if (!queue->init()) {
            return false;
        }
        _streamQueues.push_back(queue);
    }
    std::string logPrefix;
    if (agent) {
        logPrefix = agent->getLogPrefix();
    }
    if (!initLogThread(logPrefix)) {
        return false;
    }
    return !_streamQueues.empty();
}

void HeartbeatServerManager::stop() {
    _logThread.reset();
    clearBiz();
    for (const auto &queue : _streamQueues) {
        if (queue) {
            queue->stop();
        }
    }
}

bool HeartbeatServerManager::initLogThread(const std::string &logPrefix) {
    _logThread.reset();
    _logPrefix = logPrefix;
    auto logThread = autil::LoopThread::createLoopThread(
        std::bind(&HeartbeatServerManager::logThread, this), UPDATE_TIME_INTERVAL, "GigServHBLOG");
    if (!logThread) {
        AUTIL_LOG(ERROR, "start server heartbeat log thread failed");
        return false;
    }
    _logThread = logThread;
    return true;
}

void HeartbeatServerManager::logThread() {
    auto newSnapshotStr = _serverTopoMap->toString();
    if (newSnapshotStr != _snapshotStr) {
        FileRecorder::recordSnapshot(newSnapshotStr, 2000,
                                     "gig_snapshot/heartbeat_server/" + _logPrefix);
        _snapshotStr.swap(newSnapshotStr);
    }
}

bool HeartbeatServerManager::updateSpec(const Spec &spec) {
    if (_serverTopoMap->setSpec(spec)) {
        notify();
    }
    return true;
}

bool HeartbeatServerManager::publish(const ServerBizTopoInfo &info, SignatureTy &signature) {
    {
        autil::ScopedLock lock(_publishLock);
        BizTopoMapPtr topoMap;
        if (!createTopoMap(info, signature, topoMap)) {
            return false;
        }
        _serverTopoMap->setBizTopoMap(topoMap);
    }
    notify();
    return true;
}

bool HeartbeatServerManager::publish(const std::vector<ServerBizTopoInfo> &infoVec,
                                     std::vector<SignatureTy> &signatureVec) {
    {
        autil::ScopedLock lock(_publishLock);
        BizTopoMapPtr topoMap;
        if (!createTopoMap(infoVec, signatureVec, topoMap)) {
            return false;
        }
        _serverTopoMap->setBizTopoMap(topoMap);
    }
    notify();
    return true;
}

bool HeartbeatServerManager::publishGroup(PublishGroupTy group,
                                          const std::vector<ServerBizTopoInfo> &infoVec,
                                          std::vector<SignatureTy> &signatureVec) {
    if (group == INVALID_PUBLISH_GROUP) {
        AUTIL_LOG(ERROR, "invalid publish group [%lu]", group);
        return false;
    }
    {
        autil::ScopedLock lock(_publishLock);
        BizTopoMapPtr topoMap;
        if (!createReplaceTopoMap(group, infoVec, signatureVec, topoMap)) {
            return false;
        }
        _serverTopoMap->setBizTopoMap(topoMap);
    }
    notify();
    return true;
}

bool HeartbeatServerManager::updateVolatileInfo(SignatureTy signature,
                                                const BizVolatileInfo &info) {
    auto bizTopoMapPtr = _serverTopoMap->getBizTopoMap();
    if (!bizTopoMapPtr) {
        AUTIL_LOG(ERROR, "null biz top map, publish first");
        return false;
    }
    const auto &bizTopoMap = *bizTopoMapPtr;
    auto it = bizTopoMap.find(signature);
    if (bizTopoMap.end() == it) {
        AUTIL_LOG(ERROR, "update volatile info failed, signature [%lu] not exist, publish first",
                  signature);
        return false;
    }
    it->second->updateVolatileInfo(info);
    notify();
    return true;
}

bool HeartbeatServerManager::unpublish(SignatureTy signature) {
    {
        autil::ScopedLock lock(_publishLock);
        auto oldTopoMapPtr = _serverTopoMap->getBizTopoMap();
        if (!oldTopoMapPtr || (oldTopoMapPtr->end() == oldTopoMapPtr->find(signature))) {
            AUTIL_LOG(ERROR, "unpublish topo info failed, signature [%lu] not exist, publish first",
                      signature);
            return false;
        }
        auto newMapPtr = std::make_shared<BizTopoMap>(*oldTopoMapPtr);
        (*newMapPtr).erase(signature);
        if (newMapPtr->empty()) {
            newMapPtr.reset();
        }
        _serverTopoMap->setBizTopoMap(newMapPtr);
    }
    notify();
    return true;
}

bool HeartbeatServerManager::unpublish(const std::vector<SignatureTy> &signatureVec) {
    {
        autil::ScopedLock lock(_publishLock);
        auto oldTopoMapPtr = _serverTopoMap->getBizTopoMap();
        if (!oldTopoMapPtr) {
            AUTIL_LOG(ERROR,
                      "unpublish topo info vector failed, old topo map empty, publish first");
            return false;
        }
        auto newMapPtr = std::make_shared<BizTopoMap>(*oldTopoMapPtr);
        for (auto signature : signatureVec) {
            if ((newMapPtr->end() == newMapPtr->find(signature))) {
                AUTIL_LOG(ERROR,
                          "unpublish topo info failed, signature [%lu] not exist, publish first",
                          signature);
                return false;
            }
            (*newMapPtr).erase(signature);
        }
        if (newMapPtr->empty()) {
            newMapPtr.reset();
        }
        _serverTopoMap->setBizTopoMap(newMapPtr);
    }
    notify();
    return true;
}

void HeartbeatServerManager::stopAllBiz() {
    auto bizTopoMapPtr = _serverTopoMap->getBizTopoMap();
    if (!bizTopoMapPtr) {
        return;
    }
    const auto &bizTopoMap = *bizTopoMapPtr;
    for (const auto &pair : bizTopoMap) {
        pair.second->stop();
    }
    notify();
}

void HeartbeatServerManager::clearBiz() {
    _serverTopoMap->setBizTopoMap(nullptr);
    notify();
}

std::vector<ServerBizTopoInfo> HeartbeatServerManager::getPublishInfos() const {
    std::vector<ServerBizTopoInfo> ret;
    auto bizTopoMapPtr = _serverTopoMap->getBizTopoMap();
    if (!bizTopoMapPtr) {
        return ret;
    }
    for (const auto &pair : *bizTopoMapPtr) {
        ret.push_back(pair.second->getTopoInfo());
    }
    return ret;
}

std::string HeartbeatServerManager::getTopoInfoStr() const {
    return _serverTopoMap->getTopoInfoStr();
}

void HeartbeatServerManager::notify() {
    for (const auto &queue : _streamQueues) {
        queue->notify();
    }
}

bool HeartbeatServerManager::createTopoMap(const ServerBizTopoInfo &info, SignatureTy &signature,
                                           BizTopoMapPtr &topoMapPtr) {
    auto topo = createBizTopo(INVALID_PUBLISH_GROUP, info);
    if (!topo) {
        return false;
    }
    auto retMapPtr = std::make_shared<BizTopoMap>();
    auto oldTopoMapPtr = _serverTopoMap->getBizTopoMap();
    if (oldTopoMapPtr) {
        *retMapPtr = *oldTopoMapPtr;
    }
    signature = topo->getTopoSignature();
    (*retMapPtr)[signature] = topo;
    topoMapPtr = retMapPtr;
    return true;
}

bool HeartbeatServerManager::createTopoMap(const std::vector<ServerBizTopoInfo> &infoVec,
                                           std::vector<SignatureTy> &signatureVec,
                                           BizTopoMapPtr &topoMapPtr) {
    auto retMapPtr = std::make_shared<BizTopoMap>();
    auto oldTopoMapPtr = _serverTopoMap->getBizTopoMap();
    if (oldTopoMapPtr) {
        *retMapPtr = *oldTopoMapPtr;
    }
    signatureVec.clear();
    for (const auto &info : infoVec) {
        auto topo = createBizTopo(INVALID_PUBLISH_GROUP, info);
        if (!topo) {
            return false;
        }
        auto signature = topo->getTopoSignature();
        (*retMapPtr)[signature] = topo;
        signatureVec.push_back(signature);
    }
    topoMapPtr = retMapPtr;
    return true;
}

bool HeartbeatServerManager::createReplaceTopoMap(PublishGroupTy group,
                                                  const std::vector<ServerBizTopoInfo> &infoVec,
                                                  std::vector<SignatureTy> &signatureVec,
                                                  BizTopoMapPtr &topoMapPtr) {
    auto retMapPtr = std::make_shared<BizTopoMap>();
    auto &retMap = *retMapPtr;
    auto oldTopoMapPtr = _serverTopoMap->getBizTopoMap();
    if (oldTopoMapPtr) {
        for (const auto &pair : *oldTopoMapPtr) {
            const auto &topo = pair.second;
            if (INVALID_PUBLISH_GROUP == group || group != topo->getGroup()) {
                retMap.emplace(pair);
            }
        }
    }
    signatureVec.clear();
    for (const auto &info : infoVec) {
        auto topo = createBizTopo(group, info);
        if (!topo) {
            return false;
        }
        auto signature = topo->getTopoSignature();
        auto it = retMap.find(signature);
        if (retMap.end() != it) {
            AUTIL_LOG(ERROR,
                      "publish failed, duplicate topo [%s] group [0x%lx], prev group [0x%lx]",
                      topo->getId().c_str(), group, it->second->getGroup());
            return false;
        }
        retMap[signature] = topo;
        signatureVec.push_back(signature);
    }
    topoMapPtr = retMapPtr;
    return true;
}

BizTopoPtr HeartbeatServerManager::createBizTopo(PublishGroupTy group,
                                                 const ServerBizTopoInfo &info) {
    SignatureTy publishId = getNewId();
    auto topo = std::make_shared<BizTopo>(publishId, group);
    auto bizStat = getBizStat(info.bizName, info.partId);
    if (!topo->init(info, bizStat)) {
        return nullptr;
    }
    return topo;
}

std::shared_ptr<BizStat> HeartbeatServerManager::getBizStat(const std::string &bizName,
                                                            PartIdTy partId) {
    if (_agent) {
        return _agent->getBizStat(bizName, partId);
    } else {
        return nullptr;
    }
}

const ServerTopoMapPtr &HeartbeatServerManager::getServerTopoMap() const {
    return _serverTopoMap;
}

int64_t HeartbeatServerManager::getNewId() {
    return atomic_inc_return(&_idCounter);
}

void HeartbeatServerManager::addStream(const HeartbeatServerStreamPtr &stream) {
    auto queue = getQueue();
    queue->addStream(stream);
}

const HeartbeatServerStreamQueuePtr &HeartbeatServerManager::getQueue() {
    auto nextId = atomic_inc_return(&_nextQueueId);
    auto index = nextId % _streamQueues.size();
    return _streamQueues[index];
}

} // namespace multi_call
