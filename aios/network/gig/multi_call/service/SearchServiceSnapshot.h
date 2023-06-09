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
#ifndef ISEARCH_MULTI_CALL_SEARCHSERVICESNAPSHOT_H
#define ISEARCH_MULTI_CALL_SEARCHSERVICESNAPSHOT_H

#include "aios/network/gig/multi_call/common/VersionInfo.h"
#include "aios/network/gig/multi_call/common/WorkerNodeInfo.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/metric/MetricReporterManager.h"
#include "aios/network/gig/multi_call/metric/ReplyInfoCollector.h"
#include "aios/network/gig/multi_call/service/FlowConfigSnapshot.h"
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"
#include "aios/network/gig/multi_call/service/SearchServiceResource.h"
#include "aios/network/gig/multi_call/service/SearchServiceSnapshotInBiz.h"
#include "aios/network/gig/multi_call/util/RandomGenerator.h"

namespace multi_call {

typedef std::pair<SearchServiceProviderPtr, SearchServiceReplicaPtr> ProviderReplicaPair;
typedef std::unordered_map<std::string, ProviderReplicaPair> ProviderMap;
typedef std::unordered_map<std::string, std::vector<SearchServiceProviderPtr> >
    ProviderAddressMap;
typedef std::map<std::string, SearchServiceSnapshotInVersionPtr>
    BizVersionSnapshotMap;
typedef std::map<std::string, std::vector<SearchServiceSnapshotInVersionPtr> >
    BizMultiVersionSnapshotMap;

class SearchServiceSnapshot;
MULTI_CALL_TYPEDEF_PTR(SearchServiceSnapshot);

class LatencyTimeSnapshot;
MULTI_CALL_TYPEDEF_PTR(LatencyTimeSnapshot);

class SearchServiceSnapshot {
public:
    SearchServiceSnapshot(const ConnectionManagerPtr &connectionManager,
                          const MetricReporterManagerPtr &metricReporterManager,
                          const MiscConfigPtr &miscConfig);
    virtual ~SearchServiceSnapshot();

private:
    SearchServiceSnapshot(const SearchServiceSnapshot &);
    SearchServiceSnapshot &operator=(const SearchServiceSnapshot &);

public:
    bool init(const BizInfoMap &bizInfoMap,
              const SearchServiceSnapshotPtr &oldSnapshot);

    void getSpecSet(std::set<std::string> &specSet) const;
    const ProviderMap &getProviderMap() const;
    ProviderAddressMap &getAddressProviderMap(); // may update provider
    const BizSnapshotMap &getBizSnapshotMap() const;
    const MiscConfigPtr &getMiscConfig() const { return _miscConfig; }
    const ConnectionManagerPtr &getConnectionManager() const {
        return _connectionManager;
    }
    std::vector<std::string> getBizNames() const;
    bool hasCluster(const std::string &clusterName) const;
    bool hasBiz(const std::string &bizName) const;
    bool hasVersion(const std::string &bizName, VersionTy version,
                    VersionInfo &info) const;
    void fillSnapshotInfo(SnapshotInfoCollector &collector);
    SourceIdTy getRandomSourceId();
    void toString(std::string &debugStr);
    SearchServiceSnapshotInBizPtr getBizSnapshot(const std::string &bizName);
    SearchServiceSnapshotInVersionPtr
    getBizVersionSnapshot(const std::string &bizName, const VersionTy version);
    std::vector<VersionTy> getBizVersion(const std::string &bizName) const;
    std::set<VersionTy> getBizProtocalVersion(const std::string &bizName) const;
    bool disableBizNotExistLog() const;
    SearchServiceProviderPtr getProvider(const std::string &nodeId) const;
    void setLatencyTimeSnapshot(const LatencyTimeSnapshotPtr &snapshot) {
        _latencyTimeSnapshot = snapshot;
    }
private:
    // virtual for ut
    virtual void getDiffBiz(const BizInfoMap &bizInfoMap,
                            BizInfoMap &newBizInfoMap,
                            BizSnapshotMap &keepBizSnapshot);
    virtual bool constructFromBizMap(const BizInfoMap &newBizInfoMap,
                                     const ProviderMap &oldProviderMap);
    virtual void addBizSnapshot(const BizSnapshotMap &keepBizSnapshot,
                                const ProviderMap &oldProviderMap);
    bool addProvider(const TopoNode &topoNode,
                     const ProviderMap &oldProviderMap);
    SearchServiceProviderPtr createProvider(const TopoNode &topoNode,
                                            const ProviderMap &oldProviderMap);
    bool constructConsistentHash();
    void finishCreateSnapshot(const BizInfoMap &bizInfoMap) const;
private:
    void updateProviderStatus(const SearchServiceProviderPtr &provider,
                              const TopoNode &topoNode);
    void updateProviderFromOld(const SearchServiceProviderPtr &oldProvider,
                               const SearchServiceProviderPtr &newProvider);

private:
    ProviderMap _providerMap;
    ProviderAddressMap _providerAddressMap;
    BizSnapshotMap _bizSnapshotMap;
    ConnectionManagerPtr _connectionManager;
    MetricReporterManagerPtr _metricReporterManager;
    LatencyTimeSnapshotPtr _latencyTimeSnapshot;
    MiscConfigPtr _miscConfig;
    RandomGenerator _randomGenerator;
    std::set<std::string> _clusterSet;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_SEARCHSERVICESNAPSHOT_H
