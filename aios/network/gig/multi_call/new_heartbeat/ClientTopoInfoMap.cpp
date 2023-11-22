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
#include "aios/network/gig/multi_call/new_heartbeat/ClientTopoInfoMap.h"

#include "aios/network/gig/multi_call/new_heartbeat/ServerTopoMap.h"
#include "aios/network/gig/multi_call/new_heartbeat/HostHeartbeatInfo.h"
#include "aios/network/gig/multi_call/service/SearchServiceReplica.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/util/MiscUtil.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ClientTopoInfoMap);

ClientTopoInfo::ClientTopoInfo(const std::shared_ptr<HostHeartbeatStats> &hostStats)
    : _hostStats(hostStats)
    , _hasServerAgent(false)
    , _initNetLatencyUs(0) {
}

ClientTopoInfo::~ClientTopoInfo() {
    if (_provider) {
        _provider->setTargetWeight(0);
    }
}

bool ClientTopoInfo::init(const std::string &clusterName, bool enableClusterBizSearch,
                          const Spec &spec, const GigMetaEnv &envDef, const BizTopoDef &topoDef,
                          int64_t netLatencyUs) {
    const auto &keyDef = topoDef.key();
    const auto &bizName = keyDef.biz_name();
    if (enableClusterBizSearch) {
        _topoNode.bizName = MiscUtil::createBizName(clusterName, bizName);
    } else {
        _topoNode.bizName = bizName;
    }
    _topoNode.partCnt = keyDef.part_count();
    _topoNode.partId = keyDef.part_id();
    _topoNode.version = keyDef.version();
    _topoNode.protocalVersion = keyDef.protocol_version();
    _topoNode.weight = topoDef.target_weight();
    _topoNode.clusterName = clusterName;
    _topoNode.meta.targetWeight = topoDef.target_weight();
    _topoNode.meta.metaEnv.setFromProto(envDef);
    _topoNode.isValid = true;
    _topoNode.ssType = ST_HEARTBEAT;
    _topoNode.spec = spec;
    if (!_topoNode.generateNodeId()) {
        return false;
    }
    const auto &signature = topoDef.signature();
    _signature.topo = signature.topo();
    _signature.publishId = signature.publish_id();
    initMetas(signature.meta(), topoDef.metas());
    initTags(signature.tag(), topoDef.tags());
    if (topoDef.has_propagation_stat()) {
        _hasServerAgent = true;
        _firstPropagationStatDef.CopyFrom(topoDef.propagation_stat());
        _initNetLatencyUs = netLatencyUs;
    }
    return true;
}

void ClientTopoInfo::initMetas(SignatureTy sig, const BizMetasDef &metas) {
    auto newMetaMap = std::make_shared<MetaMap>();
    auto count = metas.metas_size();
    for (int32_t i = 0; i < count; i++) {
        const auto &metaDef = metas.metas(i);
        newMetaMap->emplace(metaDef.key(), metaDef.value());
    }
    if (newMetaMap->empty()) {
        newMetaMap.reset();
    }
    autil::ScopedReadWriteLock lock(_lock, 'w');
    _signature.meta = sig;
    _metas = std::move(newMetaMap);
}

void ClientTopoInfo::initTags(SignatureTy sig, const BizTagsDef &tags) {
    auto newTagMap = std::make_shared<TagMap>();
    auto count = tags.tags_size();
    for (int32_t i = 0; i < count; i++) {
        const auto &tagDef = tags.tags(i);
        newTagMap->emplace(tagDef.tag(), tagDef.version());
    }
    if (newTagMap->empty()) {
        newTagMap.reset();
    }
    autil::ScopedReadWriteLock lock(_lock, 'w');
    _signature.tag = sig;
    _tags = std::move(newTagMap);
}

bool ClientTopoInfo::update(const BizTopoDef &bizTopoDef, int64_t netLatencyUs) {
    const auto &signature = bizTopoDef.signature();
    bool needNotify = false;
    bool flush = false;
    if (_signature.meta != signature.meta()) {
        initMetas(signature.meta(), bizTopoDef.metas());
        needNotify = true;
        flush = true;
    }
    if (_signature.tag != signature.tag()) {
        initTags(signature.tag(), bizTopoDef.tags());
        flush = true;
    }
    if (_topoNode.meta.targetWeight != bizTopoDef.target_weight()) {
        _topoNode.meta.targetWeight = bizTopoDef.target_weight();
        flush = true;
    }
    if (bizTopoDef.has_propagation_stat()) {
        updatePropagationStat(bizTopoDef.propagation_stat(), netLatencyUs);
    }
    if (flush) {
        flushUpdate();
    }
    return needNotify;
}

void ClientTopoInfo::flushUpdate() {
    autil::ScopedReadWriteLock lock(_lock, 'w');
    if (!_provider) {
        return;
    }
    auto oldTargetWeight = _provider->getTargetWeight();
    auto targetWeight = _topoNode.meta.targetWeight;
    if (oldTargetWeight != targetWeight) {
        _provider->setTargetWeight(targetWeight);
    }
    if (_tags) {
        _provider->updateTagsFromMap(*_tags);
    } else {
        _provider->updateTagsFromMap(TagMap());
    }
    _provider->updateHeartbeatMetas(_metas);
    if ((oldTargetWeight != targetWeight) && (MIN_WEIGHT == targetWeight)) {
        _hostStats->ignoreNextSkip = true;
    }
}

std::shared_ptr<SearchServiceProvider> ClientTopoInfo::getProvider() const {
    autil::ScopedReadWriteLock lock(_lock, 'r');
    return _provider;
}

void ClientTopoInfo::bind(const std::shared_ptr<SearchServiceReplica> &replica,
                          const std::shared_ptr<SearchServiceProvider> &provider) {
    {
        autil::ScopedReadWriteLock lock(_lock, 'r');
        if (_replica == replica && _provider == provider) {
            return;
        }
    }
    bool flush = false;
    {
        autil::ScopedReadWriteLock lock(_lock, 'w');
        if (_replica == replica && _provider == provider) {
            return;
        }
        if (_provider != provider) {
            _provider = provider;
            provider->setHostStats(_hostStats);
            if (_hasServerAgent) {
                provider->setHasServerAgent(true);
            }
        }
        _replica = replica;
        flush = true;
    }
    if (flush) {
        flushUpdate();
    }
    if (_firstPropagationStatDef.has_latency() &&
        _firstPropagationStatDef.latency().filter_ready()) {
        updatePropagationStat(_firstPropagationStatDef, _initNetLatencyUs);
        _firstPropagationStatDef.Clear();
    }
}

void ClientTopoInfo::unBind() {
    autil::ScopedReadWriteLock lock(_lock, 'w');
    _provider.reset();
    _replica.reset();
}

void ClientTopoInfo::updatePropagationStat(const PropagationStatDef &statDef,
                                           int64_t netLatencyUs) {
    std::shared_ptr<SearchServiceProvider> provider;
    std::shared_ptr<SearchServiceReplica> replica;
    {
        autil::ScopedReadWriteLock lock(_lock, 'r');
        provider = _provider;
        replica = _replica;
    }
    SearchServiceResource::updateProviderFromHeartbeat(replica, provider, statDef, netLatencyUs);
}

void ClientTopoInfo::fillTopoRequest(TopoHeartbeatRequest &request) const {
    std::shared_ptr<SearchServiceReplica> replica;
    {
        autil::ScopedReadWriteLock lock(_lock, 'r');
        auto sigDef = request.mutable_signature();
        sigDef->set_topo(_signature.topo);
        sigDef->set_meta(_signature.meta);
        sigDef->set_tag(_signature.tag);
        sigDef->set_publish_id(_signature.publishId);
        replica = _replica;
    }
    if ((MIN_WEIGHT == _topoNode.meta.targetWeight) && replica) {
        request.set_all_provider_stopped(!replica->hasStartedProvider());
    }
}

void ClientTopoInfo::disableProvider() const {
    auto provider = getProvider();
    if (provider) {
        provider->setWeight(0.0f);
    }
}

void ClientTopoInfo::toString(std::string &debugStr, MetasSignatureMap &allMetas) const {
    std::string nodeId;
    BizTopoSignature signature;
    TagMapPtr tags;
    MetaMapPtr metas;
    std::shared_ptr<SearchServiceProvider> provider;
    std::shared_ptr<SearchServiceReplica> replica;
    {
        autil::ScopedReadWriteLock lock(_lock, 'r');
        nodeId = _topoNode.nodeId;
        signature = _signature;
        tags = _tags;
        metas = _metas;
        provider = _provider;
        replica = _replica;
    }
    debugStr += "  " + nodeId;
    debugStr += ", targetWeight: " + autil::StringUtil::toString(_topoNode.meta.targetWeight);
    debugStr += ", hasProvider: " + autil::StringUtil::toString(provider != nullptr);
    if (provider) {
        debugStr +=
            ", providerTargetWeight: " + autil::StringUtil::toString(provider->getTargetWeight());
    }
    debugStr += ", hasReplica: " + autil::StringUtil::toString(replica != nullptr);
    debugStr += ", topoSig: " + autil::StringUtil::toString(signature.topo);
    debugStr += ", tagsSig: " + autil::StringUtil::toString(signature.tag);
    if (tags) {
        debugStr += "(";
        size_t n = tags->size();
        size_t i = 0;
        for (const auto &tagPair : *tags) {
            debugStr += autil::StringUtil::toString(tagPair.first);
            debugStr += ":" + autil::StringUtil::toString(tagPair.second);
            i++;
            if (i < n) {
                debugStr += ", ";
            }
        }
        debugStr += ")";
    }
    debugStr += ", metasSig: " + autil::StringUtil::toString(signature.meta);
    if (allMetas.end() == allMetas.find(signature.meta)) {
        allMetas[signature.meta] = metas;
    }
}

ClientTopoInfoMap::ClientTopoInfoMap(uint64_t clientId) : _clientId(clientId) {
}

ClientTopoInfoMap::~ClientTopoInfoMap() {
}

bool ClientTopoInfoMap::init(const std::string &ip, const ClientTopoInfoMapPtr &oldMap,
                             const NewHeartbeatResponse &response) {
    auto serverSig = response.server_signature();
    if (oldMap) {
        auto oldServerId = oldMap->getServerId();
        if (!oldServerId) {
            AUTIL_LOG(ERROR, "BUG! null server id");
            return false;
        }
        if (oldServerId->getSignature() == serverSig) {
            _serverId = oldServerId;
            return true;
        }
    }
    if (!response.has_server_id()) {
        AUTIL_LOG(ERROR, "heartbeat response has no server_id");
        return false;
    }
    auto serverId = std::make_shared<ServerId>();
    serverId->fromProto(response.server_signature(), ip, response.server_id());
    _serverId = serverId;
    return true;
}

void ClientTopoInfoMap::addInfo(int64_t topoSig, const ClientTopoInfoPtr &info) {
    _topoMap[topoSig] = info;
}

ClientTopoInfoPtr ClientTopoInfoMap::getInfo(SignatureTy topoSig) const {
    auto it = _topoMap.find(topoSig);
    if (_topoMap.end() != it) {
        return it->second;
    }
    return nullptr;
}

bool ClientTopoInfoMap::update(const NewHeartbeatResponse &response, int64_t netLatencyUs,
                               bool &needNotify) {
    _clientId = response.client_id();
    if (_serverId->getSignature() != response.server_signature()) {
        return false;
    }
    bool isSame = true;
    auto responseCount = response.topo_size();
    for (int32_t i = 0; i < responseCount; i++) {
        const auto &topoDef = response.topo(i);
        const auto &signature = topoDef.signature();
        auto clientInfo = getInfo(signature.topo());
        if (!clientInfo) {
            isSame = false;
            continue;
        }
        if (clientInfo->update(topoDef, netLatencyUs)) {
            needNotify = true;
        }
    }
    return isSame && ((size_t)responseCount == _topoMap.size());
}

void ClientTopoInfoMap::fillRequest(NewHeartbeatRequest &request) const {
    request.set_client_id(_clientId);
    request.set_server_signature(_serverId->getSignature());
    for (const auto &pair : _topoMap) {
        const auto &clientInfo = *pair.second;
        auto topoRequest = request.add_topo_request();
        clientInfo.fillTopoRequest(*topoRequest);
    }
}

bool ClientTopoInfoMap::fillBizInfoMap(BizInfoMap &bizInfoMap) const {
    for (const auto &pair : _topoMap) {
        auto &topoNode = pair.second->getTopoNode();
        topoNode.clientInfo = pair.second;
        bizInfoMap[topoNode.bizName].emplace(topoNode);
        topoNode.clientInfo.reset();
    }
    return true;
}

void ClientTopoInfoMap::disableAllProvider() const {
    for (const auto &pair : _topoMap) {
        pair.second->disableProvider();
    }
}

void ClientTopoInfoMap::toString(std::string &debugStr, MetasSignatureMap &allMetas) const {
    debugStr += ", clientId: " + autil::StringUtil::toString(_clientId);
    debugStr += ", serverSig: " + autil::StringUtil::toString(_serverId->getSignature());
    debugStr += ", topoCount: " + autil::StringUtil::toString(_topoMap.size());
    for (const auto &pair : _topoMap) {
        debugStr += "\n";
        pair.second->toString(debugStr, allMetas);
    }
}
} // namespace multi_call
