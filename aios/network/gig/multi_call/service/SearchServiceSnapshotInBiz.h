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
#ifndef ISEARCH_MULTI_CALL_SEARCHSERVICESNAPSHOTINBIZ_H
#define ISEARCH_MULTI_CALL_SEARCHSERVICESNAPSHOTINBIZ_H

#include <unordered_map>

#include "aios/network/gig/multi_call/common/VersionInfo.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"
#include "aios/network/gig/multi_call/metric/SnapshotInfoCollector.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "aios/network/gig/multi_call/service/SearchServiceSnapshotInVersion.h"

namespace multi_call {

class SearchServiceSnapshotInBiz
{
public:
    SearchServiceSnapshotInBiz(const std::string &bizName, const MiscConfigPtr &miscConfig);
    ~SearchServiceSnapshotInBiz();

private:
    SearchServiceSnapshotInBiz(const SearchServiceSnapshotInBiz &);
    SearchServiceSnapshotInBiz &operator=(const SearchServiceSnapshotInBiz &);

public:
    bool init(const MetricReporterManagerPtr &metricReporterManager);
    bool addProvider(const TopoNode &topoNode, const SearchServiceProviderPtr &provider,
                     SearchServiceReplicaPtr &retReplica);
    bool constructConsistentHash();
    void toString(std::string &debugStr);
    void fillSnapshotBizInfo(SnapshotBizInfo &bizInfo);
    bool hasDiff(const std::set<TopoNode> &topoNodes);
    const std::set<TopoNode> &getTopoNodes() const {
        return _topoNodes;
    }
    const VersionSnapshotMap &getVersionSnapshotMap() const {
        return _versionSnapshotMap;
    }
    const VersionSnapshotMap &getCopyVersionSnapshotMap() const {
        return _copyVersionSnapshotMap;
    }
    size_t getNormalProviderCount() const {
        return _normalProviderCount;
    }
    std::vector<VersionTy> getVersion() {
        std::vector<VersionTy> result;
        for (auto item : _versionSnapshotMap) {
            result.push_back(item.second->getVersion());
        }
        return result;
    }
    std::set<VersionTy> getProtocalVersion() {
        std::set<VersionTy> result;
        if (!_versionSnapshotMap.empty()) {
            for (auto &item : _versionSnapshotMap) {
                result.insert(item.second->getProtocalVersion());
            }
        }
        return result;
    }
    const std::vector<std::pair<VersionTy, WeightTy>> &getVersionNormalProviderCountVec() const {
        return _versionNormalProviderCountVec;
    }
    bool hasVersion(VersionTy version, VersionInfo &info) const;
    void getMetaInfo(BizMetaInfo &metaInfo) const;
    SearchServiceSnapshotInVersionPtr getVersionSnapshot(VersionTy version);
    std::string getBizName() const {
        return _bizName;
    }
    bool active() const {
        return _active;
    }
    void setActive() {
        _active = true;
    }

private:
    static void doToString(const VersionSnapshotMap &versionSnapshotMap, bool isCopy,
                           std::string &debugStr);

private:
    std::string _bizName;
    MiscConfigPtr _miscConfig;
    VersionSnapshotMap _versionSnapshotMap;
    VersionSnapshotMap _copyVersionSnapshotMap;
    std::set<TopoNode> _topoNodes;
    RandomGenerator _randomGenerator;
    size_t _normalProviderCount;
    std::vector<std::pair<VersionTy, WeightTy>> _versionNormalProviderCountVec;
    bool _active; // has query in this biz
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(SearchServiceSnapshotInBiz);

typedef std::unordered_map<std::string, SearchServiceSnapshotInBizPtr> BizSnapshotMap;

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_SEARCHSERVICESNAPSHOTINBIZ_H
