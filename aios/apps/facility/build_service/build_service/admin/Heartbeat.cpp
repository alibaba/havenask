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
#include "build_service/admin/Heartbeat.h"

#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;
using namespace build_service::proto;
namespace build_service { namespace admin {
BS_LOG_SETUP(admin, Heartbeat);

Heartbeat::Heartbeat() {}

Heartbeat::~Heartbeat() {}

bool Heartbeat::start(int64_t interval)
{
    _loopThreadPtr = LoopThread::createLoopThread(bind(&Heartbeat::syncStatus, this), interval);
    return _loopThreadPtr.get() != NULL;
}

void Heartbeat::syncNodesStatus(const WorkerNodes& nodes)
{
    ScopedLock lock(_mutex);
    HeartbeatCache newCache;
    for (auto it = nodes.begin(); it != nodes.end(); ++it) {
        if (!(*it)->isReady() || (*it)->isFinished()) {
            continue;
        }
        const PartitionId& partitionId = (*it)->getPartitionId();
        HeartbeatCache::const_iterator cacheIt = _heartbeatCache.find(partitionId);
        if (cacheIt == _heartbeatCache.end()) {
            newCache[partitionId] = Status();
        } else {
            newCache[partitionId] = cacheIt->second;
        }
        Status& status = newCache[partitionId];
        (*it)->updateHeartbeatTime(status.statusUpdateTime);
        (*it)->updateWorkerStartTime(status.workerStartTime);
        (*it)->getTargetStatusStr(status.target, status.targetIdentifier);
        (*it)->fillTargetDependResources(status.targetResources);
        // todo receive current resources
        if (status.targetIdentifier.empty() || status.targetIdentifier == status.currentIdentifier) {
            (*it)->setCurrentResources(status.usingResources);
            (*it)->setCurrentStatusStr(status.current, status.currentIdentifier);
        } else {
            (*it)->setCurrentResources(status.usingResources);
            (*it)->setCurrentStatusStr("", "");
        }
    }
    _heartbeatCache = newCache;
}

void Heartbeat::stop() { _loopThreadPtr.reset(); }

void Heartbeat::setUsingResources(const PartitionId& partitionId, const std::string& resources)
{
    ScopedLock lock(_mutex);
    HeartbeatCache::iterator it = _heartbeatCache.find(partitionId);
    if (it == _heartbeatCache.end()) {
        BS_LOG(DEBUG, "partition not exist [%s]", partitionId.ShortDebugString().c_str());
        return;
    }
    it->second.usingResources = resources;
}

void Heartbeat::setCurrentStatus(const PartitionId& partitionId, const string& currentStatus,
                                 const string& currentIdentifier, int64_t startTimestamp)
{
    ScopedLock lock(_mutex);
    HeartbeatCache::iterator it = _heartbeatCache.find(partitionId);
    if (it == _heartbeatCache.end()) {
        BS_LOG(DEBUG, "partition not exist [%s]", partitionId.ShortDebugString().c_str());
        return;
    }
    const auto& targetIdentifier = it->second.targetIdentifier;
    if (!targetIdentifier.empty() && targetIdentifier != currentIdentifier) {
        BS_LOG(WARN, "host mismatch for part  [%s], targetIdentifier [%s], currentIdentifier [%s],p drop current",
               partitionId.ShortDebugString().c_str(), targetIdentifier.c_str(), currentIdentifier.c_str());
        return;
    }
    it->second.currentIdentifier = currentIdentifier;
    it->second.current = currentStatus;
    it->second.statusUpdateTime = autil::TimeUtility::currentTimeInSeconds();
    it->second.workerStartTime = startTimestamp;
}

void Heartbeat::getTargetStatus(const PartitionId& partitionId, string& target, string& requiredIdentifier)
{
    ScopedLock lock(_mutex);
    HeartbeatCache::iterator it = _heartbeatCache.find(partitionId);
    if (it == _heartbeatCache.end()) {
        return;
    }
    target = it->second.target;
    requiredIdentifier = it->second.targetIdentifier;
}

const std::string& Heartbeat::getDependResource(const PartitionId& partitionId)
{
    ScopedLock lock(_mutex);
    HeartbeatCache::iterator it = _heartbeatCache.find(partitionId);
    if (it == _heartbeatCache.end()) {
        static std::string emptyStr;
        return emptyStr;
    }
    return it->second.targetResources;
}

vector<PartitionId> Heartbeat::getAllPartIds() const
{
    vector<PartitionId> partitionIds;
    ScopedLock lock(_mutex);
    partitionIds.reserve(_heartbeatCache.size());
    for (HeartbeatCache::const_iterator it = _heartbeatCache.begin(); it != _heartbeatCache.end(); ++it) {
        partitionIds.push_back(it->first);
    }
    return partitionIds;
}

void Heartbeat::setTargetStatus(const proto::PartitionId& partitionId, const std::string& targetStatus)
{
    ScopedLock lock(_mutex);
    // insert for test
    _heartbeatCache[partitionId].target = targetStatus;
}

string Heartbeat::getCurrentStatus(const PartitionId& partitionId)
{
    ScopedLock lock(_mutex);
    HeartbeatCache::iterator it = _heartbeatCache.find(partitionId);
    if (it == _heartbeatCache.end()) {
        return string();
    }
    return it->second.current;
}

}} // namespace build_service::admin
