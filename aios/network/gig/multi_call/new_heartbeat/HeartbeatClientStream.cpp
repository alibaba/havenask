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
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatClientStream.h"

#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatClientManager.h"
#include "aios/network/gig/multi_call/new_heartbeat/HostHeartbeatInfo.h"
#include "aios/network/gig/multi_call/new_heartbeat/ServerTopoMap.h"
#include "aios/network/gig/multi_call/proto/NewHeartbeat.pb.h"

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatClientStream);

HeartbeatClientStream::HeartbeatClientStream(const std::shared_ptr<HostHeartbeatStats> &hostStats,
                                             const ClientTopoInfoMapPtr &clientInfoMap,
                                             const std::string &clusterName,
                                             bool enableClusterBizSearch)
    : GigClientStream(GIG_NEW_HEARTBEAT_METHOD_NAME, GIG_HEARTBEAT_METHOD_NAME)
    , _hostStats(hostStats)
    , _notifier(hostStats->notifier)
    , _clientInfoMap(clientInfoMap)
    , _clusterName(clusterName)
    , _enableClusterBizSearch(enableClusterBizSearch) {
    setDisableRetry(true);
    setDisableProbe(true);
    AUTIL_LOG(DEBUG, "heartbeat client stream constructed [%p]", this);
}

HeartbeatClientStream::~HeartbeatClientStream() {
    AUTIL_LOG(DEBUG, "heartbeat client stream destructed [%p]", this);
}

google::protobuf::Message *
HeartbeatClientStream::newReceiveMessage(google::protobuf::Arena *arena) const {
    return google::protobuf::Arena::CreateMessage<NewHeartbeatResponse>(arena);
}

bool HeartbeatClientStream::receive(const GigStreamMessage &message) {
    auto response = dynamic_cast<NewHeartbeatResponse *>(message.message);
    if (!response) {
        AUTIL_LOG(ERROR, "null heartbeat msg");
        return false;
    }
    AUTIL_LOG(DEBUG, "message received: %s, netLatencyUs: %ld, host: %s",
              response->DebugString().c_str(), message.netLatencyUs,
              getSpec().getAnetSpec(MC_PROTOCOL_GRPC_STREAM).c_str());
    if (doReceive(*response, message.netLatencyUs)) {
        _hostStats->updateLastHeartbeatTime(true);
        return true;
    } else {
        _hostStats->updateLastHeartbeatTime(false);
        return false;
    }
}

void HeartbeatClientStream::receiveCancel(const GigStreamMessage &message, MultiCallErrorCode ec) {
    _hostStats->updateLastHeartbeatTime(false);
    AUTIL_LOG(WARN, "receive cancel, host [%s] disabled, this [%p], ec [%s]",
              getSpec().getAnetSpec(MC_PROTOCOL_GRPC_STREAM).c_str(), this, translateErrorCode(ec));
    disableProvider();
    _notifier->notify();
}

void HeartbeatClientStream::notifyIdle(PartIdTy partId) {
}

bool HeartbeatClientStream::tick() {
    NewHeartbeatRequest request;
    fillRequest(request);
    if (send(0, false, &request)) {
        AUTIL_LOG(DEBUG, "send gig heartbeat request success [%p] [%s]", this,
                  request.DebugString().c_str());
        return true;
    } else {
        _hostStats->updateLastHeartbeatTime(false);
        AUTIL_LOG(ERROR, "send gig heartbeat request failed, host [%s] [%p] [%s]",
                  getSpec().getAnetSpec(MC_PROTOCOL_GRPC_STREAM).c_str(), this,
                  request.DebugString().c_str());
        return false;
    }
}

void HeartbeatClientStream::disableProvider() {
    auto clientMap = getClientMap();
    if (clientMap) {
        clientMap->disableAllProvider();
        AUTIL_LOG(WARN, "server stopped or timeout, host [%s] disabled, this [%p]",
                  getSpec().getAnetSpec(MC_PROTOCOL_GRPC_STREAM).c_str(), this);
    }
}

void HeartbeatClientStream::fillRequest(NewHeartbeatRequest &request) {
    request.set_rpc_version(GIG_STREAM_HEARTBEAT_VERSION);
    auto clientMap = getClientMap();
    if (clientMap) {
        clientMap->fillRequest(request);
    }
}

bool HeartbeatClientStream::doReceive(const NewHeartbeatResponse &response, int64_t netLatencyUs) {
    auto oldMap = getClientMap();
    bool needNotify = false;
    if (oldMap && oldMap->update(response, netLatencyUs, needNotify)) {
        if (needNotify) {
            _notifier->notify();
        }
        return true;
    }
    auto newClientId = response.client_id();
    auto newMap = std::make_shared<ClientTopoInfoMap>(newClientId);
    if (!newMap->init(getSpec().ip, oldMap, response)) {
        return false;
    }
    const auto &spec = newMap->getServerId()->getSpec();
    auto count = response.topo_size();
    const auto &envDef = response.server_id().env();
    for (int32_t i = 0; i < count; i++) {
        const auto &topoDef = response.topo(i);
        const auto &signature = topoDef.signature();
        auto publishId = signature.publish_id();
        auto topoSig = signature.topo();
        auto oldTopoInfo = getOldInfo(oldMap, topoSig);
        if (oldTopoInfo && (publishId == oldTopoInfo->getPublishId())) {
            newMap->addInfo(topoSig, oldTopoInfo);
        } else {
            if (oldTopoInfo) {
                // disable setTargetWeight in old info destructor
                oldTopoInfo->unBind();
            }
            auto newInfo = std::make_shared<ClientTopoInfo>(_hostStats);
            if (newInfo->init(_clusterName, _enableClusterBizSearch, spec, envDef, topoDef,
                              netLatencyUs)) {
                newMap->addInfo(topoSig, newInfo);
            } else {
                AUTIL_LOG(ERROR,
                          "invalid topo info [%s] this [%p], host [%s], response [%s], oldMap [%p]",
                          newInfo->getTopoNode().nodeId.c_str(), this,
                          getSpec().getAnetSpec(MC_PROTOCOL_GRPC_STREAM).c_str(),
                          response.DebugString().c_str(), oldMap.get());
                return false;
            }
        }
    }
    oldMap.reset();
    setClientMap(newMap);
    _notifier->notify();
    return true;
}

void HeartbeatClientStream::setClientMap(const ClientTopoInfoMapPtr &newMap) {
    autil::ScopedReadWriteLock lock(_clientMapLock, 'w');
    _clientInfoMap = newMap;
}

ClientTopoInfoMapPtr HeartbeatClientStream::getClientMap() const {
    autil::ScopedReadWriteLock lock(_clientMapLock, 'r');
    return _clientInfoMap;
}

ClientTopoInfoPtr HeartbeatClientStream::getOldInfo(const ClientTopoInfoMapPtr &clientMap,
                                                    SignatureTy topoSig) const {
    if (clientMap) {
        return clientMap->getInfo(topoSig);
    } else {
        return nullptr;
    }
}
} // namespace multi_call
