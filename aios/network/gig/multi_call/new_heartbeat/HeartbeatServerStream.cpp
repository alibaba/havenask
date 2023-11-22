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
#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatServerStream.h"

#include "aios/network/gig/multi_call/new_heartbeat/HeartbeatServerManager.h"
#include "aios/network/gig/multi_call/new_heartbeat/ServerTopoMap.h"
#include "aios/network/gig/multi_call/proto/NewHeartbeat.pb.h"

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatServerStream);

HeartbeatServerStream::HeartbeatServerStream(const HeartbeatServerManagerPtr &manager)
    : _clientId(manager->getNewId())
    , _serverTopoMap(manager->getServerTopoMap())
    , _inited(false)
    , _cancelled(false) {
    AUTIL_LOG(DEBUG, "new client connection received, clientId [%lu], this [%p] manager [%p]",
              _clientId, this, manager.get());
}

HeartbeatServerStream::~HeartbeatServerStream() {
    AUTIL_LOG(DEBUG, "client connection closed, clientId [%lu], [%p]", _clientId, this);
}

google::protobuf::Message *
HeartbeatServerStream::newReceiveMessage(google::protobuf::Arena *arena) const {
    return google::protobuf::Arena::CreateMessage<NewHeartbeatRequest>(arena);
}

bool HeartbeatServerStream::receive(const multi_call::GigStreamMessage &message) {
    _inited = true;
    auto heartbeatMessage = dynamic_cast<NewHeartbeatRequest *>(message.message);
    if (!heartbeatMessage) {
        AUTIL_LOG(ERROR, "null heartbeat msg");
        return false;
    }
    AUTIL_LOG(DEBUG, "message received, clientId: %lu, msg: %s", _clientId,
              heartbeatMessage->DebugString().c_str());
    updateClientSignatureMap(*heartbeatMessage);
    flush(false);
    return true;
}

void HeartbeatServerStream::updateClientSignatureMap(const NewHeartbeatRequest &request) {
    auto newSignatureMap = std::make_shared<SignatureMap>();
    newSignatureMap->clientId = request.client_id();
    newSignatureMap->serverSignature = request.server_signature();
    auto count = request.topo_request_size();
    for (int32_t i = 0; i < count; i++) {
        const auto &topoRequest = request.topo_request(i);
        const auto &sigDef = topoRequest.signature();
        BizTopoClientInfo clientInfo;
        clientInfo.allProviderStopped = topoRequest.all_provider_stopped();
        auto &sig = clientInfo.sig;
        sig.topo = sigDef.topo();
        sig.meta = sigDef.meta();
        sig.tag = sigDef.tag();
        sig.publishId = sigDef.publish_id();
        newSignatureMap->topoSignatureMap[sig.topo] = clientInfo;
    }
    setClientSignatureMap(newSignatureMap);
}

SignatureMapPtr HeartbeatServerStream::getClientSignatureMap() const {
    autil::ScopedLock scope(_signatureMutex);
    return _clientSignatureMap;
}

void HeartbeatServerStream::setClientSignatureMap(const SignatureMapPtr &newMap) {
    autil::ScopedLock scope(_signatureMutex);
    _clientSignatureMap = newMap;
}

void HeartbeatServerStream::receiveCancel(const multi_call::GigStreamMessage &message,
                                          multi_call::MultiCallErrorCode ec) {
    _inited = true;
    _cancelled = true;
    AUTIL_LOG(INFO, "cancel message received, clientId: %lu", _clientId);
    autil::ScopedLock lock(_flushMutex);
    if (0 != _flushMutex.trylock()) {
        return;
    }
    sendCancel(0, nullptr);
    _flushMutex.unlock();
}

bool HeartbeatServerStream::flush(bool sync) {
    if (!_inited) {
        return true;
    }
    if (_cancelled) {
        return false;
    }
    if (sync) {
        _flushMutex.lock();
    } else if (0 != _flushMutex.trylock()) {
        return false;
    }
    NewHeartbeatResponse response;
    fillResponse(response);
    auto ret = send(0, false, &response);
    if (!ret) {
        sendCancel(0, nullptr);
        _cancelled = true;
    }
    _flushMutex.unlock();
    AUTIL_LOG(DEBUG, "server heartbeat: %s", response.DebugString().c_str());
    return ret;
}

void HeartbeatServerStream::stop() {
    sendCancel(0, nullptr);
}

void HeartbeatServerStream::fillResponse(NewHeartbeatResponse &response) {
    response.set_rpc_version(GIG_STREAM_HEARTBEAT_VERSION);
    response.set_client_id(_clientId);
    auto clientSignatureMap = getClientSignatureMap();
    auto serverId = _serverTopoMap->getServerId();
    if (!serverId) {
        return;
    }
    serverId->fillServerIdDef(clientSignatureMap, response);
    auto topoMapPtr = _serverTopoMap->getBizTopoMap();
    if (topoMapPtr) {
        const auto &topoMap = *topoMapPtr;
        for (const auto &pair : topoMap) {
            const auto &topo = *pair.second;
            auto topoDef = response.add_topo();
            if (clientSignatureMap) {
                const auto &signatureMap = clientSignatureMap->topoSignatureMap;
                const auto &id = pair.first;
                auto it = signatureMap.find(id);
                if (signatureMap.end() != it) {
                    const auto &clientSignature = it->second.sig;
                    if (clientSignature.publishId == topo.getPublishId()) {
                        topo.fillBizTopoDiff(clientSignature, topoDef);
                        topo.fillPropagationStat(topoDef);
                        continue;
                    }
                }
            }
            topo.fillBizTopo(topoDef);
            topo.fillPropagationStat(topoDef);
        }
    }
}

bool HeartbeatServerStream::isAllReplicaStopped(SignatureTy sig) {
    auto clientSignatureMap = getClientSignatureMap();
    if (!clientSignatureMap) {
        return false;
    }
    const auto &signatureMap = clientSignatureMap->topoSignatureMap;
    auto it = signatureMap.find(sig);
    if (signatureMap.end() != it) {
        return it->second.allProviderStopped;
    }
    return false;
}

} // namespace multi_call
