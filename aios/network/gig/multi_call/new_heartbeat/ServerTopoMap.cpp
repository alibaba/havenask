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
#include "aios/network/gig/multi_call/new_heartbeat/ServerTopoMap.h"
#include "aios/network/gig/multi_call/common/TopoInfoBuilder.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringConvertor.h"
#include "autil/StringUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ServerId);
AUTIL_LOG_SETUP(multi_call, ServerTopoMap);

void ServerId::init(const Spec &spec) {
    _spec = spec;
    if (!_metaEnv.init()) {
        AUTIL_LOG(WARN, "meta env init failed");
    }
    initSignature();
}

void ServerId::initSignature() {
    int32_t maxLength = 1024;
    autil::StringAppender appender(maxLength);
    GigMetaEnv gigMetaEnv;
    _metaEnv.fillProto(gigMetaEnv);
    appender.appendString(gigMetaEnv.SerializeAsString());
    for (size_t i = 0; i < MC_PROTOCOL_UNKNOWN; i++) {
        appender.appendChar('_').appendInt64(_spec.ports[i]);
    }
    std::string id;
    appender.copyToString(id);
    _signature = autil::HashAlgorithm::hashString64(id.c_str(), id.size());
}

void ServerId::fillServerIdDef(const SignatureMapPtr &clientSignatureMap,
                               NewHeartbeatResponse &response) const
{
    response.set_server_signature(_signature);
    if (clientSignatureMap && (clientSignatureMap->serverSignature == _signature)) {
        return;
    }
    auto &serverIdDef = *response.mutable_server_id();
    _metaEnv.fillProto(*serverIdDef.mutable_env());

    auto &specDef = *serverIdDef.mutable_spec();
    specDef.set_tcp_port(_spec.ports[MC_PROTOCOL_TCP]);
    specDef.set_arpc_port(_spec.ports[MC_PROTOCOL_ARPC]);
    specDef.set_http_port(_spec.ports[MC_PROTOCOL_HTTP]);
    specDef.set_grpc_port(_spec.ports[MC_PROTOCOL_GRPC]);
    specDef.set_grpc_stream_port(_spec.ports[MC_PROTOCOL_GRPC_STREAM]);
    specDef.set_rdma_arpc_port(_spec.ports[MC_PROTOCOL_RDMA_ARPC]);
}

void ServerId::fromProto(SignatureTy signature, const std::string &ip, const ServerIdDef &def) {
    _signature = signature;
    _metaEnv.setFromProto(def.env());
    const auto &specDef = def.spec();
    _spec.ip = ip;
    _spec.ports[MC_PROTOCOL_TCP] = specDef.tcp_port();
    _spec.ports[MC_PROTOCOL_ARPC] = specDef.arpc_port();
    _spec.ports[MC_PROTOCOL_HTTP] = specDef.http_port();
    _spec.ports[MC_PROTOCOL_GRPC] = specDef.grpc_port();
    _spec.ports[MC_PROTOCOL_GRPC_STREAM] = specDef.grpc_stream_port();
    _spec.ports[MC_PROTOCOL_RDMA_ARPC] = specDef.rdma_arpc_port();
}

void ServerId::toString(std::string &ret) const {
    _metaEnv.toString(ret);
    ret += "ip: " + _spec.ip + "\n";
    for (uint32_t type = 0; type < MC_PROTOCOL_UNKNOWN; ++type) {
        ret += convertProtocolTypeToStr((ProtocolType)type) +
               "_port: " + autil::StringUtil::toString(_spec.ports[type]) + "\n";
    }
    ret += "server_signature: " + autil::StringUtil::toString(_signature);
}

ServerTopoMap::ServerTopoMap() {
}

ServerTopoMap::~ServerTopoMap() {
}

bool ServerTopoMap::setSpec(const Spec &spec) {
    if (!spec.hasValidPort()) {
        return false;
    }
    auto serverId = std::make_shared<ServerId>();
    serverId->init(spec);
    setServerId(serverId);
    return true;
}

void ServerTopoMap::setServerId(const ServerIdPtr &serverId) {
    autil::ScopedReadWriteLock lock(_lock, 'w');
    _serverId = serverId;
}

ServerIdPtr ServerTopoMap::getServerId() const {
    autil::ScopedReadWriteLock lock(_lock, 'r');
    return _serverId;
}

void ServerTopoMap::setBizTopoMap(const BizTopoMapPtr &newMap) {
    autil::ScopedReadWriteLock lock(_lock, 'w');
    _bizTopoMap = newMap;
}

BizTopoMapPtr ServerTopoMap::getBizTopoMap() const {
    autil::ScopedReadWriteLock lock(_lock, 'r');
    return _bizTopoMap;
}

std::string ServerTopoMap::getTopoInfoStr() const {
    auto serverId = getServerId();
    if (!serverId) {
        return "";
    }
    const auto &spec = serverId->getSpec();
    TopoInfoBuilder builder(spec.ports[MC_PROTOCOL_GRPC_STREAM]);
    auto topoMapPtr = getBizTopoMap();
    if (topoMapPtr) {
        const auto &topoMap = *topoMapPtr;
        for (const auto &pair : topoMap) {
            pair.second->addToBuilder(builder);
        }
    }
    return builder.build();
}

std::string ServerTopoMap::toString() const {
    std::string ret;
    auto serverId = getServerId();
    if (!serverId) {
        ret += "empty server id";
    }else {
        serverId->toString(ret);
    }
    ret += "\n";
    auto bizTopoMapPtr = getBizTopoMap();
    if (!bizTopoMapPtr) {
        return ret;
    }
    const auto &topoMap = *bizTopoMapPtr;
    ret += "\nbiz_list:\n";
    for (const auto &pair : topoMap) {
        pair.second->toString(ret);
        ret += "\n";
    }
    return ret;
}

}
