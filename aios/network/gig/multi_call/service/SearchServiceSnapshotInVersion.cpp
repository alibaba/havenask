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
#include "aios/network/gig/multi_call/service/SearchServiceSnapshotInVersion.h"

#include <algorithm>
#include <limits>
#include <numeric>

#include "aios/network/gig/multi_call/metric/SnapshotInfoCollector.h"
#include "aios/network/gig/multi_call/service/FlowControlParam.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, SearchServiceSnapshotInVersion);

SearchServiceSnapshotInVersion::SearchServiceSnapshotInVersion(VersionTy version, PartIdTy partCnt,
                                                               const MiscConfigPtr &miscConfig)
    : _version(version)
    , _partCnt(partCnt)
    , _weight(0)
    , _lastUpateTime(-1)
    , _normalProviderCount(0)
    , _copyProviderCount(0)
    , _miscConfig(miscConfig) {
}

SearchServiceSnapshotInVersion::~SearchServiceSnapshotInVersion() {
}

bool SearchServiceSnapshotInVersion::addSearchServiceProvider(
    PartIdTy partId, const SearchServiceProviderPtr &provider,
    SearchServiceReplicaPtr &retReplica) {
    SearchServiceReplicaPtr replicaPtr;
    ReplicaMap::iterator iter = _replicaMap.find(partId);
    if (iter != _replicaMap.end()) {
        replicaPtr = iter->second;
    } else {
        replicaPtr.reset(new SearchServiceReplica(_miscConfig));
        replicaPtr->setBizName(_bizName);
        _replicaMap[partId] = replicaPtr;
        replicaPtr->setPartId(partId);
    }
    if (!replicaPtr->addSearchServiceProvider(provider)) {
        AUTIL_LOG(WARN, "replica addSearchServiceProvider failed");
        return false;
    }
    retReplica = replicaPtr;
    if (provider->getWeight() >= 0) {
        _normalProviderCount++;
    } else {
        _copyProviderCount++;
    }
    return true;
}

void SearchServiceSnapshotInVersion::selectProvider(PartIdTy partId, SourceIdTy sourceId,
                                                    const FlowControlParam &param,
                                                    const MatchTagMapPtr &matchTagMap,
                                                    SearchServiceReplicaPtr &replica,
                                                    SearchServiceProviderPtr &provider,
                                                    SearchServiceProviderPtr &probeProvider,
                                                    RequestType &type) {
    auto iter = _replicaMap.find(partId);
    if (iter == _replicaMap.end()) {
        return;
    }
    replica = iter->second;
    provider =
        iter->second->getProviderByHashKey(sourceId, param, matchTagMap, probeProvider, type);
}

void SearchServiceSnapshotInVersion::selectProbeProvider(PartIdTy partId,
                                                         const MatchTagMapPtr &matchTagMap,
                                                         SearchServiceReplicaPtr &replica,
                                                         SearchServiceProviderPtr &provider) {
    auto iter = _replicaMap.find(partId);
    if (iter == _replicaMap.end()) {
        return;
    }
    replica = iter->second;
    RequestType type;
    provider = iter->second->getCopyProvider(matchTagMap, type);
}

SearchServiceProviderPtr SearchServiceSnapshotInVersion::getBackupProvider(
    const SearchServiceProviderPtr &provider, PartIdTy partId, SourceIdTy sourceId,
    const MatchTagMapPtr &matchTagMap, const FlowControlConfigPtr &flowControlConfig) {
    ReplicaMap::iterator iter = _replicaMap.find(partId);
    if (iter == _replicaMap.end()) {
        return SearchServiceProviderPtr();
    }
    const SearchServiceReplicaPtr &replica = iter->second;
    return replica->getBackupProvider(provider, sourceId, matchTagMap, flowControlConfig);
}

void SearchServiceSnapshotInVersion::getWeightInfo(int64_t currentTime, WeightTy &weight,
                                                   VersionInfo &info) {
    {
        ScopedReadWriteLock lock(_lock, 'r');
        if (_lastUpateTime != -1 && currentTime - _lastUpateTime < MULTI_CALL_UPDATE_INTERVAL) {
            weight = _weight;
            info = _versionInfo;
            return;
        }
    }
    getWeightInfoByPart(weight, info, {});
    ScopedReadWriteLock lock(_lock, 'w');
    _weight = weight;
    _versionInfo = info;
    _lastUpateTime = currentTime;
}

void SearchServiceSnapshotInVersion::getWeightInfoByPart(WeightTy &weight, VersionInfo &info,
                                                         const PartRequestMap &partMap) {
    info.clear();
    if (unlikely(_replicaMap.empty())) {
        weight = 0;
        AUTIL_LOG(ERROR, "replicaMap is empty");
        return;
    }

    info.partCount = partMap.empty() ? _partCnt : partMap.size();

    info.subscribeCount = _replicaMap.size();
    if (!partMap.empty()) {
        info.subscribeCount = 0;
        for (auto &part : partMap) {
            if (_replicaMap.find(part.first) != _replicaMap.end()) {
                info.subscribeCount++;
            }
        }
    }

    WeightTy minWeight = std::numeric_limits<WeightTy>::max();
    WeightTy sumWeight = 0;
    for (const auto &replica : _replicaMap) {
        if (!partMap.empty() && partMap.find(replica.first) == partMap.end()) {
            continue;
        }
        if (replica.second->hasHealthProvider()) {
            info.healthCount++;
        }
        if (!replica.second->hasStartedProvider()) {
            info.stopCount++;
        }
        info.normalProviderCount += replica.second->getNormalProviderCount();
        auto w = replica.second->getReplicaWeight();
        minWeight = min(minWeight, w);
        sumWeight += w;
    }
    weight = info.complete() ? minWeight : sumWeight;
}

bool SearchServiceSnapshotInVersion::isComplete() {
    WeightTy weight = 0;
    VersionInfo info;
    getWeightInfo(autil::TimeUtility::currentTime(), weight, info);
    return info.complete();
}

bool SearchServiceSnapshotInVersion::constructConsistentHash(bool multiVersion) {
    if (isCopyVersion()) {
        return true;
    }
    for (auto it = _replicaMap.begin(); it != _replicaMap.end();) {
        const auto &replica = it->second;
        if (replica->isCopyReplica()) {
            it = _replicaMap.erase(it);
        } else {
            it++;
        }
    }
    for (const auto &replica : _replicaMap) {
        if (!replica.second->constructConsistentHash(multiVersion)) {
            return false;
        }
    }
    return true;
}

void SearchServiceSnapshotInVersion::fillVersionInfo(SnapshotBizInfo &bizInfo) {
    int32_t weight = 0;
    VersionInfo info;
    getWeightInfo(TimeUtility::currentTime(), weight, info);
    bizInfo.totalVersionWeight += weight;
    if (info.complete()) {
        bizInfo.completeVersionWeight += weight;
        bizInfo.completeVersionCount++;
    }
    for (const auto &replica : _replicaMap) {
        replica.second->fillReplicaInfo(bizInfo);
    }
}

void SearchServiceSnapshotInVersion::toString(std::string &debugStr) {
    auto iter = _replicaMap.begin();
    for (; iter != _replicaMap.end(); ++iter) {
        debugStr += "    partId:";
        debugStr += StringUtil::toString(iter->first);
        iter->second->toString(debugStr);
    }
}

} // namespace multi_call
