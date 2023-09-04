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
#include "aios/network/gig/multi_call/service/SearchServiceSnapshotInBiz.h"

#include <utility>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, SearchServiceSnapshotInBiz);

SearchServiceSnapshotInBiz::SearchServiceSnapshotInBiz(const string &bizName,
                                                       const MiscConfigPtr &miscConfig)
    : _bizName(bizName)
    , _miscConfig(miscConfig)
    , _randomGenerator(autil::TimeUtility::currentTimeInMicroSeconds())
    , _normalProviderCount(0)
    , _active(false) {
    assert(_miscConfig);
}

SearchServiceSnapshotInBiz::~SearchServiceSnapshotInBiz() {
}

bool SearchServiceSnapshotInBiz::init(const MetricReporterManagerPtr &metricReporterManager) {
    // create metric reporter
    if (metricReporterManager && _active) {
        // maybe null in java client
        metricReporterManager->getBizMetricReporter(_bizName);
    }
    return true;
}

bool SearchServiceSnapshotInBiz::addProvider(const TopoNode &topoNode,
                                             const SearchServiceProviderPtr &provider,
                                             SearchServiceReplicaPtr &retReplica) {
    SearchServiceSnapshotInVersionPtr versionSnapshotPtr;
    auto iter = _versionSnapshotMap.find(topoNode.version);
    if (iter != _versionSnapshotMap.end()) {
        versionSnapshotPtr = iter->second;
    } else {
        versionSnapshotPtr.reset(
            new SearchServiceSnapshotInVersion(topoNode.version, topoNode.partCnt, _miscConfig));
        versionSnapshotPtr->setBizName(_bizName);
        _versionSnapshotMap[topoNode.version] = versionSnapshotPtr;
    }
    PartIdTy expectPartCnt = versionSnapshotPtr->getPartCount();
    if (topoNode.partCnt != expectPartCnt) {
        AUTIL_LOG(WARN, "add provider in biz [%s] fail, partCnt [%d], expect [%d].",
                  _bizName.c_str(), topoNode.partCnt, expectPartCnt)
        return false;
    }
    if (!versionSnapshotPtr->addSearchServiceProvider(topoNode.partId, provider, retReplica)) {
        return false;
    }
    if (provider->getWeight() >= 0) {
        _normalProviderCount++;
    }
    _topoNodes.insert(topoNode);
    if (provider->getTotalQueryCounter() > 0) {
        _active = true;
    }
    return true;
}

bool SearchServiceSnapshotInBiz::hasDiff(const set<TopoNode> &topoNodes) {
    return _topoNodes != topoNodes;
}

bool SearchServiceSnapshotInBiz::hasVersion(VersionTy version, VersionInfo &info) const {
    auto it = _versionSnapshotMap.find(version);
    if (_versionSnapshotMap.end() != it) {
        const auto &versionSnapshot = it->second;
        if (!versionSnapshot->isCopyVersion()) {
            WeightTy weight = 0;
            versionSnapshot->getWeightInfo(autil::TimeUtility::currentTime(), weight, info);
            return true;
        }
    }
    info.clear();
    return false;
}

void SearchServiceSnapshotInBiz::getMetaInfo(BizMetaInfo &metaInfo) const {
    // sorted for diff
    for (const auto &pair : _versionSnapshotMap) {
        auto version = pair.first;
        const auto &versionSnapshot = pair.second;
        if (!versionSnapshot->isCopyVersion()) {
            VersionInfo info;
            WeightTy weight = 0;
            versionSnapshot->getWeightInfo(autil::TimeUtility::currentTime(), weight, info);
            versionSnapshot->fillMeta(info);
            metaInfo.versions.emplace(version, std::move(info));
        }
    }
}

bool SearchServiceSnapshotInBiz::constructConsistentHash() {
    _versionNormalProviderCountVec.clear();
    for (auto it = _versionSnapshotMap.begin(); it != _versionSnapshotMap.end();) {
        if (it->second->isCopyVersion()) {
            _copyVersionSnapshotMap.insert(*it);
            it = _versionSnapshotMap.erase(it);
        } else {
            WeightTy count = it->second->getNormalProviderCount();
            _versionNormalProviderCountVec.push_back(make_pair(it->first, count));
            it++;
        }
    }
    for (const auto &versionSnapshot : _versionSnapshotMap) {
        if (!versionSnapshot.second->constructConsistentHash(_versionNormalProviderCountVec.size() >
                                                             1)) {
            return false;
        }
    }
    return true;
}

void SearchServiceSnapshotInBiz::toString(std::string &debugStr) {
    doToString(_versionSnapshotMap, false, debugStr);
    doToString(_copyVersionSnapshotMap, true, debugStr);
}

void SearchServiceSnapshotInBiz::doToString(const VersionSnapshotMap &versionSnapshotMap,
                                            bool isCopy, std::string &debugStr) {
    auto iter = versionSnapshotMap.begin();
    for (; iter != versionSnapshotMap.end(); ++iter) {
        WeightTy weight = 0;
        VersionInfo info;
        iter->second->getWeightInfo(autil::TimeUtility::currentTime(), weight, info);

        debugStr += "  version:";
        debugStr += StringUtil::toString(iter->first);
        debugStr += ", partCount:";
        debugStr += StringUtil::toString(iter->second->getPartCount());
        debugStr += ", isComplete:";
        debugStr += StringUtil::toString(info.complete());
        debugStr += ", isCopy:";
        debugStr += StringUtil::toString(isCopy);
        debugStr += ", weight: ";
        debugStr += StringUtil::toString(weight);
        debugStr += "\n";
        iter->second->toString(debugStr);
    }
}

void SearchServiceSnapshotInBiz::fillSnapshotBizInfo(SnapshotBizInfo &bizInfo) {
    bizInfo.versionCount = _versionSnapshotMap.size();
    bizInfo.copyVersionCount = _copyVersionSnapshotMap.size();
    for (const auto &version : _versionSnapshotMap) {
        version.second->fillVersionInfo(bizInfo);
    }
    for (const auto &version : _copyVersionSnapshotMap) {
        bizInfo.copyProviderCount += version.second->getCopyProviderCount();
    }
}

SearchServiceSnapshotInVersionPtr
SearchServiceSnapshotInBiz::getVersionSnapshot(VersionTy version) {
    auto iter = _versionSnapshotMap.find(version);
    if (iter != _versionSnapshotMap.end()) {
        return iter->second;
    } else {
        return SearchServiceSnapshotInVersionPtr();
    }
}

} // namespace multi_call
