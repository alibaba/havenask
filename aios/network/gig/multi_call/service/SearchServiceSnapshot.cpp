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
#include "aios/network/gig/multi_call/service/SearchServiceSnapshot.h"

#include "aios/network/gig/multi_call/new_heartbeat/ClientTopoInfoMap.h"
#include "aios/network/gig/multi_call/service/LatencyTimeSnapshot.h"
#include "autil/TimeUtility.h"

using namespace std;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, SearchServiceSnapshot);

SearchServiceSnapshot::SearchServiceSnapshot(const ConnectionManagerPtr &connectionManager,
                                             const MetricReporterManagerPtr &metricReporterManager,
                                             const MiscConfigPtr &miscConfig)
    : _connectionManager(connectionManager)
    , _metricReporterManager(metricReporterManager)
    , _miscConfig(miscConfig)
    , _randomGenerator(autil::TimeUtility::currentTimeInMicroSeconds()) {
    assert(_miscConfig);
}

SearchServiceSnapshot::~SearchServiceSnapshot() {
}

void SearchServiceSnapshot::getDiffBiz(const BizInfoMap &bizInfoMap, BizInfoMap &newBizInfoMap,
                                       BizSnapshotMap &keepBizSnapshot) {
    for (const auto &bizInfo : bizInfoMap) {
        auto it = _bizSnapshotMap.find(bizInfo.first);
        if (_bizSnapshotMap.end() == it || it->second->hasDiff(bizInfo.second)) {
            newBizInfoMap.insert(bizInfo);
        } else {
            keepBizSnapshot[bizInfo.first] = it->second;
        }
    }
}

bool SearchServiceSnapshot::init(const BizInfoMap &bizInfoMap,
                                 const SearchServiceSnapshotPtr &oldSnapshot) {
    size_t nodeCount = bizInfoMap.size();
    _providerMap.reserve(nodeCount);
    _providerAddressMap.reserve(nodeCount);

    if (oldSnapshot) {
        BizInfoMap newBizInfoMap;
        BizSnapshotMap keepBizSnapshot;
        oldSnapshot->getDiffBiz(bizInfoMap, newBizInfoMap, keepBizSnapshot);
        const auto &oldProviderMap = oldSnapshot->getProviderMap();
        if (!constructFromBizMap(newBizInfoMap, oldProviderMap)) {
            return false;
        }
        addBizSnapshot(keepBizSnapshot, oldProviderMap);
    } else {
        if (!constructFromBizMap(bizInfoMap, ProviderMap())) {
            return false;
        }
    }
    finishCreateSnapshot(bizInfoMap);
    return true;
}

void SearchServiceSnapshot::finishCreateSnapshot(const BizInfoMap &bizInfoMap) const {
    for (const auto &bizPair : bizInfoMap) {
        const auto &topoSet = bizPair.second;
        for (const auto &topoNode : topoSet) {
            if (topoNode.clientInfo) {
                auto it = _providerMap.find(topoNode.nodeId);
                if (it == _providerMap.end()) {
                    continue;
                }
                const auto &pair = it->second;
                topoNode.clientInfo->bind(pair.second, pair.first);
            }
        }
    }
}

bool SearchServiceSnapshot::constructFromBizMap(const BizInfoMap &newBizInfoMap,
                                                const ProviderMap &oldProviderMap) {
    for (const auto &bizInfo : newBizInfoMap) {
        for (const auto &topoNode : bizInfo.second) {
            if (!addProvider(topoNode, oldProviderMap)) {
                return false;
            }
        }
    }
    return constructConsistentHash();
}

void SearchServiceSnapshot::addBizSnapshot(const BizSnapshotMap &keepBizSnapshot,
                                           const ProviderMap &oldProviderMap) {
    for (const auto &bizSnapshot : keepBizSnapshot) {
        _bizSnapshotMap.insert(bizSnapshot);
        const auto &topoNodes = bizSnapshot.second->getTopoNodes();
        for (const auto &node : topoNodes) {
            const auto &id = node.nodeId;
            auto it = oldProviderMap.find(id);
            if (oldProviderMap.end() == it) {
                AUTIL_LOG(ERROR, "find nodeId[%s] in oldProviderMap failed", id.c_str());
                continue;
            }
            _providerMap[id] = it->second;
            const auto &provider = it->second.first;
            if (provider->supportHeartbeat()) {
                _providerAddressMap[provider->getHeartbeatSpec()].emplace_back(provider);
            }
            _clusterSet.insert(node.clusterName);
        }
    }
}

bool SearchServiceSnapshot::addProvider(const TopoNode &topoNode,
                                        const ProviderMap &oldProviderMap) {
    auto provider = createProvider(topoNode, oldProviderMap);
    if (!provider) {
        AUTIL_LOG(ERROR, "create provider failed");
        return false;
    }
    if (!topoNode.isValid && !provider->hasServerAgent()) {
        // provider dropped
        return true;
    }
    SearchServiceSnapshotInBizPtr bizSnapshot;
    auto it = _bizSnapshotMap.find(topoNode.bizName);
    if (_bizSnapshotMap.end() == it) {
        bizSnapshot.reset(new SearchServiceSnapshotInBiz(topoNode.bizName, _miscConfig));
        if (!bizSnapshot->init(_metricReporterManager)) {
            return false;
        }
        _bizSnapshotMap[topoNode.bizName] = bizSnapshot;
    } else {
        bizSnapshot = it->second;
    }
    SearchServiceReplicaPtr retReplica;
    if (!bizSnapshot->addProvider(topoNode, provider, retReplica)) {
        return false;
    }
    _providerMap[topoNode.nodeId] = std::make_pair(provider, retReplica);
    if (provider->supportHeartbeat()) {
        _providerAddressMap[provider->getHeartbeatSpec()].emplace_back(provider);
    }
    _clusterSet.insert(topoNode.clusterName);
    return true;
}

SearchServiceProviderPtr SearchServiceSnapshot::createProvider(const TopoNode &topoNode,
                                                               const ProviderMap &oldProviderMap) {
    SearchServiceProviderPtr oldProvider;
    auto it = oldProviderMap.find(topoNode.nodeId);
    if (oldProviderMap.end() != it) {
        oldProvider = it->second.first;
    }
    if (oldProvider && topoNode.meta == oldProvider->getTopoNodeMeta()) {
        updateProviderStatus(oldProvider, topoNode);
        return oldProvider;
    } else {
        SearchServiceProviderPtr newProvider(
            new SearchServiceProvider(_connectionManager, topoNode));
        updateProviderFromOld(oldProvider, newProvider);
        return newProvider;
    }
}

void SearchServiceSnapshot::updateProviderStatus(const SearchServiceProviderPtr &provider,
                                                 const TopoNode &topoNode) {
    assert(provider);
    provider->setClusterName(topoNode.clusterName);
    provider->updateValidState(topoNode.isValid);
    if (topoNode.hbInfo) {
        HbMetaInfoPtr hbMetaInfo = topoNode.hbInfo->getHbMeta();
        provider->updateHeartbeatInfo(hbMetaInfo);
    }
    provider->setSubscribeType(topoNode.ssType);
}

void SearchServiceSnapshot::updateProviderFromOld(const SearchServiceProviderPtr &oldProvider,
                                                  const SearchServiceProviderPtr &newProvider) {
    if (!oldProvider || !newProvider) {
        return;
    }
    MetaEnv oldMetaEnv = oldProvider->getNodeMetaEnv();
    MetaEnv newMetaEnv = newProvider->getNodeMetaEnv();
    if (oldMetaEnv.valid() && !newMetaEnv.valid()) {
        newProvider->setNodeMetaEnv(oldMetaEnv);
    }
}

SourceIdTy SearchServiceSnapshot::getRandomSourceId() {
    return _randomGenerator.get();
}

bool SearchServiceSnapshot::constructConsistentHash() {
    for (const auto &bizSnapshot : _bizSnapshotMap) {
        if (!bizSnapshot.second->constructConsistentHash()) {
            return false;
        }
    }
    return true;
}

void SearchServiceSnapshot::fillSnapshotInfo(SnapshotInfoCollector &collector) {
    for (const auto &bizSnapshot : _bizSnapshotMap) {
        if (bizSnapshot.second->active()) { // only record biz with querys
            SnapshotBizInfo bizInfo;
            bizSnapshot.second->fillSnapshotBizInfo(bizInfo);
            collector.addSnapshotBizInfo(bizSnapshot.first, bizInfo);
        }
    }
}

vector<string> SearchServiceSnapshot::getBizNames() const {
    vector<string> bizNames;
    bizNames.reserve(_bizSnapshotMap.size());
    for (const auto &biz : _bizSnapshotMap) {
        bizNames.push_back(biz.first);
    }
    return bizNames;
}

bool SearchServiceSnapshot::hasCluster(const std::string &clusterName) const {
    return _clusterSet.end() != _clusterSet.find(clusterName);
}

bool SearchServiceSnapshot::hasBiz(const std::string &bizName) const {
    return _bizSnapshotMap.end() != _bizSnapshotMap.find(bizName);
}

bool SearchServiceSnapshot::hasVersion(const std::string &bizName, VersionTy version,
                                       VersionInfo &info) const {
    auto it = _bizSnapshotMap.find(bizName);
    if (_bizSnapshotMap.end() == it) {
        info.clear();
        return false;
    }
    return it->second->hasVersion(version, info);
}

void SearchServiceSnapshot::getBizMetaInfos(std::vector<BizMetaInfo> &bizMetaInfos) const {
    // sorted for diff
    std::map<std::string, SearchServiceSnapshotInBizPtr> sortMap;
    for (const auto &pair : _bizSnapshotMap) {
        sortMap.insert(pair);
    }
    for (const auto &bizPair : sortMap) {
        BizMetaInfo info;
        info.bizName = bizPair.first;
        bizPair.second->getMetaInfo(info);
        bizMetaInfos.emplace_back(std::move(info));
    }
}

SearchServiceSnapshotInBizPtr SearchServiceSnapshot::getBizSnapshot(const string &bizName) {
    auto iter = _bizSnapshotMap.find(bizName);
    if (iter != _bizSnapshotMap.end()) {
        return iter->second;
    } else {
        if (!disableBizNotExistLog()) {
            AUTIL_LOG(ERROR, "biz [%s] not exist", bizName.c_str());
        }
        return SearchServiceSnapshotInBizPtr();
    }
}

SearchServiceSnapshotInVersionPtr
SearchServiceSnapshot::getBizVersionSnapshot(const string &bizName, const VersionTy version) {
    auto bizSnapshot = getBizSnapshot(bizName);
    if (bizSnapshot) {
        return bizSnapshot->getVersionSnapshot(version);
    }
    return SearchServiceSnapshotInVersionPtr();
}

vector<VersionTy> SearchServiceSnapshot::getBizVersion(const std::string &bizName) const {
    auto iter = _bizSnapshotMap.find(bizName);
    if (_bizSnapshotMap.end() == iter) {
        return vector<VersionTy>();
    }
    return iter->second->getVersion();
}

std::set<VersionTy> SearchServiceSnapshot::getBizProtocalVersion(const std::string &bizName) const {
    auto iter = _bizSnapshotMap.find(bizName);
    if (_bizSnapshotMap.end() == iter) {
        return std::set<VersionTy>();
    }
    return iter->second->getProtocalVersion();
}

void SearchServiceSnapshot::toString(std::string &debugStr) {
    debugStr += "[\n";
    std::map<std::string, SearchServiceSnapshotInBizPtr> sortMap;
    for (const auto &pair : _bizSnapshotMap) {
        sortMap.insert(pair);
    }
    for (const auto &pair : sortMap) {
        const auto &bizName = pair.first;
        const auto &bizSnapshot = pair.second;
        auto versionCount = bizSnapshot->getVersionSnapshotMap().size();
        debugStr += "biz:";
        debugStr += bizName;
        debugStr += ", versionCount: ";
        debugStr += autil::StringUtil::toString(versionCount);
        if (_latencyTimeSnapshot) {
            debugStr += ", avgLatency: ";
            debugStr += autil::StringUtil::fToString(_latencyTimeSnapshot->getAvgLatency(bizName) /
                                                     1000.0f);
        }
        debugStr += "\n";
        bizSnapshot->toString(debugStr);
        debugStr += "\n";
    }
    debugStr += "]\n";
}

const ProviderMap &SearchServiceSnapshot::getProviderMap() const {
    return _providerMap;
}

ProviderAddressMap &SearchServiceSnapshot::getAddressProviderMap() {
    return _providerAddressMap;
}

const BizSnapshotMap &SearchServiceSnapshot::getBizSnapshotMap() const {
    return _bizSnapshotMap;
}

void SearchServiceSnapshot::getSpecSet(std::set<std::string> &specSet) const {
    for (const auto &item : _providerMap) {
        item.second.first->collectSpecs(specSet);
    }
}

bool SearchServiceSnapshot::disableBizNotExistLog() const {
    return _miscConfig && _miscConfig->disableBizNotExistLog;
}

SearchServiceProviderPtr SearchServiceSnapshot::getProvider(const std::string &nodeId) const {
    auto iter = _providerMap.find(nodeId);
    if (iter != _providerMap.end()) {
        return iter->second.first;
    } else {
        return SearchServiceProviderPtr();
    }
}

} // namespace multi_call
