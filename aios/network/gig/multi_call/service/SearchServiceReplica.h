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
#ifndef ISEARCH_MULTI_CALL_SEARCHSERVICEREPLICA_H
#define ISEARCH_MULTI_CALL_SEARCHSERVICEREPLICA_H
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "aios/network/gig/multi_call/service/ReplicaController.h"
#include "aios/network/gig/multi_call/util/ConsistentHash.h"
#include "aios/network/gig/multi_call/util/RandomHash.h"
#include "aios/network/gig/multi_call/util/DiffCounter.h"
#include "autil/Lock.h"

namespace multi_call {

struct ControllerChain;
class FlowControlParam;
struct SnapshotBizInfo;

class SearchServiceReplica {
public:
    SearchServiceReplica(const MiscConfigPtr &miscConfig);
    ~SearchServiceReplica();

private:
    SearchServiceReplica(const SearchServiceReplica &);
    SearchServiceReplica &operator=(const SearchServiceReplica &);

public:
    bool constructConsistentHash(bool multiVersion);
    bool addSearchServiceProvider(
        const SearchServiceProviderPtr &searchServiceProviderPtr);
    SearchServiceProviderPtr getProviderByHashKey(SourceIdTy key, const FlowControlParam &param,
                                                  const MatchTagMapPtr &matchTagMap,
                                                  SearchServiceProviderPtr &probeProvider,
                                                  RequestType &type);
    SearchServiceProviderPtr getCopyProvider(const MatchTagMapPtr &matchTagMap, RequestType &type);
    SearchServiceProviderPtr getBackupProvider(const SearchServiceProviderPtr &provider,
                                               SourceIdTy sourceId,
                                               const MatchTagMapPtr &matchTagMap,
                                               const FlowControlConfigPtr &flowControlConfig);
    SearchServiceProviderPtr getProviderByAgentId(uint64_t agentId);
    void fillPropagationStat(uint64_t skipAgentId, google::protobuf::Arena *arena,
                             PropagationStats &propagationStats);
    ControllerChain *getBestChain() const;
    float getAvgWeight() const;
    ReplicaController *getReplicaController() { return &_replicaController; }
    WeightTy getReplicaWeight();
    bool hasHealthProvider() const;
    bool hasStartedProvider() const;
    void toString(std::string &debugStr);
    void fillReplicaInfo(SnapshotBizInfo &bizInfo) const;
    void setPartId(PartIdTy partId) { _partId = partId; }
    PartIdTy getPartId() const { return _partId; }
    bool isCopyReplica() const { return _serviceVector.empty(); }
    size_t getNormalProviderCount() const { return _serviceVector.size(); }
    size_t getHeartbeatHealthCount() const;
    VersionTy getProtocalVersion() const {
        if (!_serviceVector.empty()) {
            return _serviceVector[0]->getProtocalVersion();
        }
        return -1;
    }
    void setBizName(const std::string &bizName) {
        _bizName = bizName;
    }
    FlowControlConfigPtr getLastFlowControlConfig() const;
private:
    SearchServiceProviderPtr
    findNextProviderInRing(const SearchServiceProviderVector &serviceVector,
                           ConsistentHash::Iterator it);
    bool needDegrade(uint64_t key, const FlowControlParam &param,
                     SearchServiceProviderPtr &probeProvider,
                     RequestType &type);
    bool needDegrade(uint64_t key, const FlowControlParam &param,
                     float percent);
    bool needProbe(const SearchServiceProviderPtr &provider,
                   const FlowControlConfigPtr &flowControlConfig,
                   RequestType &type);
    SearchServiceProviderPtr
    getProviderFromConsistHash(const SearchServiceProviderVector &serviceVector, SourceIdTy key,
                               const FlowControlParam &param,
                               SearchServiceProviderPtr &probeProvider, RequestType &type);
    SearchServiceProviderPtr
    getProviderFromRandomHash(const SearchServiceProviderVector &serviceVector, SourceIdTy key,
                              const FlowControlParam &param,
                              SearchServiceProviderPtr &probeProvider, RequestType &type);
    uint32_t getRandomHashWeights(const SearchServiceProviderVector &serviceVector,
                                  std::vector<RandomHashNode> &weights) const;
    bool getDegradeCoverPercent(const FlowControlConfigPtr &flowControlConfig,
                                float &baseAvgLatency, float &baseErrorRatio,
                                float &percent);
    bool getLatencyCoverPercent(const FlowControlConfigPtr &flowControlConfig,
                                float &baseAvgLatency, float &percent);
    bool
    getErrorRatioCoverPercent(const FlowControlConfigPtr &flowControlConfig,
                              float &baseErrorRatio, float &percent);
    SearchServiceProviderPtr
    getProviderFromAllByRandomHash(const SearchServiceProviderVector &serviceVector,
                                   SourceIdTy key);
    size_t getStartIndex(const SearchServiceProviderVector &serviceVector, SourceIdTy key) const;
    SearchServiceProviderPtr getProviderByIndex(uint64_t index);
    uint64_t getRandom() {
        return _randomGenerator.get();
    }
    void fillTagMaxVersion(const MatchTagMapPtr &matchTagMap) const;
    bool getProviderVecByMatchTag(const MatchTagMapPtr &matchTagMap,
                                  SearchServiceProviderVector &requireVec,
                                  SearchServiceProviderVector &preferVec);
    void trySetLastFlowControlConfig(const FlowControlConfigPtr &config);
private:
    static bool doFillPropagationStat(const SearchServiceProviderPtr &provider,
                                      PropagationStatDef &propagationStat);
private:
    typedef std::map<std::string, SearchServiceProviderPtr> ProviderMap;

private:
    std::string _bizName;
    SearchServiceProviderVector _serviceVector;
    std::unordered_map<uint64_t, SearchServiceProviderPtr> _providerMapByAgentId;
    SearchServiceProviderVector _serviceVectorCopy;
    ConsistentHash _providerHashRing;
    RandomGenerator _randomGenerator;
    ProviderMap _providerMap;
    MiscConfigPtr _miscConfig;
    HashPolicy _hashPolicy;
    PartIdTy _partId;
    ReplicaController _replicaController;
    Filter _degradeRatio;
    DiffCounter _queryCounter;
    mutable autil::ReadWriteLock _lastFlowControlConfigLock;
    FlowControlConfigPtr _lastFlowControlConfig;
private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(SearchServiceReplica);
typedef std::vector<SearchServiceReplica> SearchServiceReplicaVector;
} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_SEARCHSERVICEREPLICA_H
