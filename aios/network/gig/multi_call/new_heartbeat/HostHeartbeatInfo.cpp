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
#include "aios/network/gig/multi_call/new_heartbeat/HostHeartbeatInfo.h"
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatClientManager.h"
#include "aios/network/gig/multi_call/interface/SearchService.h"
#include "multi_call/new_heartbeat/HeartbeatClientStream.h"
#include "aios/network/gig/multi_call/proto/NewHeartbeat.pb.h"

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HostHeartbeatStats);
AUTIL_LOG_SETUP(multi_call, HostHeartbeatInfo);

void HostHeartbeatStats::updateLastResponseTime() {
    lastResponseTime = autil::TimeUtility::currentTime();
}

void HostHeartbeatStats::updateLastHeartbeatTime(bool isSuccess) {
    AUTIL_LOG(DEBUG, "heartbeat completed for host [%s]", addr.c_str());
    success = isSuccess;
    bool needNotify = !topoReady && notifier;
    topoReady = true;
    lastHeartbeatTime = autil::TimeUtility::currentTime();
    if (needNotify) {
        notifier->notifyHeartbeatSuccess();
    }
}

HostHeartbeatInfo::HostHeartbeatInfo()
    : _searchService(nullptr)
    , _skipCount(0)
    , _totalCount(0)
    , _stats(std::make_shared<HostHeartbeatStats>())
{
}

HostHeartbeatInfo::~HostHeartbeatInfo() {
    stop();
}

void HostHeartbeatInfo::stop() {
    auto stream = getStream();
    if (stream) {
        stream->sendCancel(0, nullptr);
        setStream(nullptr);
    }
}

bool HostHeartbeatInfo::init(const std::shared_ptr<HeartbeatClientManagerNotifier> &notifier,
                             const std::shared_ptr<SearchServiceSnapshot> &heartbeatSnapshot,
                             SearchService *searchService,
                             const HeartbeatSpec &spec)
{
    if (getStream()) {
        return true;
    }
    if (!searchService) {
        return false;
    }
    _stats->notifier = notifier;
    _heartbeatSnapshot = heartbeatSnapshot;
    _searchService = searchService;
    _spec = spec;
    _stats->addr = _spec.spec.getAnetSpec(MC_PROTOCOL_GRPC_STREAM);
    if (!createStream()) {
        _stats->updateLastHeartbeatTime(false);
        return false;
    }
    if (!tick()) {
        _stats->updateLastHeartbeatTime(false);
        return false;
    }
    return true;
}

bool HostHeartbeatInfo::createStream() {
    auto stream = newClientStream();
    if (!_searchService->bind(stream, _heartbeatSnapshot)) {
        AUTIL_LOG(ERROR, "bind heartbeat stream failed, spec[%s]",
                  _spec.spec.getAnetSpec(MC_PROTOCOL_GRPC_STREAM).c_str());
        return false;
    }
    setStream(stream);
    return true;
}

std::shared_ptr<HeartbeatClientStream> HostHeartbeatInfo::newClientStream() const {
    ClientTopoInfoMapPtr oldClientMap;
    auto oldStream = getStream();
    if (oldStream) {
        oldClientMap = oldStream->getClientMap();
    }
    auto stream = std::make_shared<HeartbeatClientStream>(_stats, oldClientMap, _spec.clusterName,
                                                          _spec.enableClusterBizSearch);
    stream->setTimeout(INFINITE_TIMEOUT);
    stream->setSpec(_spec.spec);
    stream->setForceStop(true);
    return stream;
}

bool HostHeartbeatInfo::tick() {
    if (skip(autil::TimeUtility::currentTime())) {
        return true;
    }
    _totalCount++;
    auto stream = getStream();
    if (!stream) {
        if (!createStream()) {
            AUTIL_LOG(ERROR, "reconnect heartbeat stream failed");
            return false;
        }
    }
    stream = getStream();
    if (!stream) {
        return false;
    }
    if (!stream->tick()) {
        stream->sendCancel(0, nullptr);
        createStream();
        return false;
    }
    return true;
}

bool HostHeartbeatInfo::skip(int64_t currentTime) {
    if ((0 == _stats->lastHeartbeatTime) || !_stats->success) {
        _skipCount = 0;
        return false;
    }
    auto heartbeatDiff = currentTime - _stats->lastHeartbeatTime;
    if (heartbeatDiff < GIG_HEARTBEAT_SKIP_LOW) {
        return true;
    }
    if (0 == _stats->lastResponseTime) {
        return skipByCount();
    }
    auto queryDiff = currentTime - _stats->lastResponseTime;
    if (queryDiff < GIG_HEARTBEAT_SKIP_LOW) {
        return skipByCount();
    }
    if (queryDiff > GIG_HEARTBEAT_SKIP_HIGH) {
        return skipByCount();
    }
    _skipCount = 0;
    return false;
}

bool HostHeartbeatInfo::skipByCount() {
    _skipCount++;
    if (_skipCount > 30) {
        _skipCount = 0;
        return false;
    }
    return true;
}

bool HostHeartbeatInfo::fillBizInfoMap(BizInfoMap &bizInfoMap) const {
    if (!_stats->topoReady) {
        AUTIL_LOG(INFO, "heartbeat not completed for host [%s]",
                  _spec.spec.getAnetSpec(MC_PROTOCOL_GRPC_STREAM).c_str());
        return false;
    }
    auto stream = getStream();
    if (!stream) {
        return true;
    }
    auto clientInfoMap = stream->getClientMap();
    if (!clientInfoMap) {
        return true;
    }
    clientInfoMap->fillBizInfoMap(bizInfoMap);
    return true;
}

bool HostHeartbeatInfo::isTopoReady() const {
    return _stats->topoReady;
}

void HostHeartbeatInfo::setStream(const HeartbeatClientStreamPtr &stream) {
    autil::ScopedReadWriteLock lock(_streamLock, 'w');
    _stream = stream;
}

HeartbeatClientStreamPtr HostHeartbeatInfo::getStream() const {
    autil::ScopedReadWriteLock lock(_streamLock, 'r');
    return _stream;
}

void HostHeartbeatInfo::toString(std::string &debugStr, MetasSignatureMap &allMetas) const {
    debugStr += _spec.spec.getAnetSpec(MC_PROTOCOL_GRPC_STREAM);
    debugStr += ", cluster: " + _spec.clusterName;
    debugStr += ", topoReady: " + autil::StringUtil::toString(_stats->topoReady);
    debugStr += ", success: " + autil::StringUtil::toString(_stats->success);
    debugStr += ", lastHeartbeat: " + getTimeStrFromUs(_stats->lastHeartbeatTime);
    debugStr += ", lastResponse: " + getTimeStrFromUs(_stats->lastResponseTime);
    debugStr += ", totalCount: " + autil::StringUtil::toString(_totalCount);
    debugStr += ", skipCount: " + autil::StringUtil::toString(_skipCount);
    auto stream = getStream();
    if (!stream) {
        return;
    }
    auto clientMap = stream->getClientMap();
    if (clientMap) {
        clientMap->toString(debugStr, allMetas);
    }
    debugStr += "\n";
}
}
