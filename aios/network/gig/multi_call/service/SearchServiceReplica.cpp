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
#include "aios/network/gig/multi_call/service/SearchServiceReplica.h"

#include "aios/network/gig/multi_call/metric/SnapshotInfoCollector.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "aios/network/gig/multi_call/service/FlowControlParam.h"
#include "autil/HashAlgorithm.h"

using namespace std;
using namespace autil;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, SearchServiceReplica);

SearchServiceReplica::SearchServiceReplica(const MiscConfigPtr &miscConfig)
    : _randomGenerator(autil::TimeUtility::currentTimeInMicroSeconds())
    , _miscConfig(miscConfig)
    , _partId(INVALID_PART_ID)
    , _replicaController(&_serviceVector)
    , _degradeRatio(FILTER_INIT_LIMIT) {
    assert(_miscConfig);
    _hashPolicy = _miscConfig->getHashPolicy();
}

SearchServiceReplica::~SearchServiceReplica() {
}

bool SearchServiceReplica::addSearchServiceProvider(
    const SearchServiceProviderPtr &searchServiceProviderPtr) {
    if (!searchServiceProviderPtr) {
        return false;
    }
    const auto &nodeId = searchServiceProviderPtr->getNodeId();
    auto weight = searchServiceProviderPtr->getWeight();
    auto it = _providerMap.find(nodeId);
    if (_providerMap.end() != it) {
        AUTIL_LOG(WARN, "service already exist in replica, nodeId[%s]", nodeId.c_str());
        if (weight >= MIN_WEIGHT) {
            return false;
        } else {
            return true;
        }
    }
    if (weight < MIN_WEIGHT) {
        _serviceVectorCopy.push_back(searchServiceProviderPtr);
    } else {
        _serviceVector.push_back(searchServiceProviderPtr);
        auto agentId = searchServiceProviderPtr->getAgentId();
        _providerMapByAgentId.insert(make_pair(agentId, searchServiceProviderPtr));
    }
    _providerMap[nodeId] = searchServiceProviderPtr;
    return true;
}

bool SearchServiceReplica::constructConsistentHash(bool multiVersion) {
    if (isCopyReplica()) {
        return false;
    }
    if (HashPolicy::CONSISTENT_HASH == _hashPolicy) {
        _providerHashRing.init(_serviceVector.size(), _miscConfig->virtualNodeCount);

        for (size_t i = 0; i < _serviceVector.size(); ++i) {
            const auto &nodeId = _serviceVector[i]->getNodeId();
            uint64_t hashKey = HashAlgorithm::hashString64(nodeId.c_str(), nodeId.size());
            _providerHashRing.add(i, hashKey);
        }
        _providerHashRing.construct();
    }
    auto bestChainQueueSize = min(MAX_BEST_CHAIN_QUEUE_SIZE, _serviceVector.size());
    _replicaController.init(bestChainQueueSize, multiVersion);
    return true;
}

SearchServiceProviderPtr SearchServiceReplica::getProviderByHashKey(
    SourceIdTy key, const FlowControlParam &param, const MatchTagMapPtr &matchTagMap,
    SearchServiceProviderPtr &probeProvider, RequestType &type) {
    _queryCounter.inc();
    if (_serviceVector.empty()) {
        probeProvider = getCopyProvider(matchTagMap, type);
        return SearchServiceProviderPtr();
    }
    trySetLastFlowControlConfig(param.flowControlConfig);
    if (needDegrade(key, param, probeProvider, type)) {
        _degradeRatio.update(MAX_RATIO);
        if (probeProvider && matchTagMap) {
            if (TML_NOT_MATCH == probeProvider->matchTags(matchTagMap)) {
                probeProvider.reset();
            }
        }
        return SearchServiceProviderPtr();
    }
    _degradeRatio.update(MIN_RATIO);
    SearchServiceProviderVector *providerVecPtr = &_serviceVector;
    SearchServiceProviderVector requireVec;
    SearchServiceProviderVector preferVec;
    if (unlikely((bool)matchTagMap)) {
        auto usePrefer = getProviderVecByMatchTag(matchTagMap, requireVec, preferVec);
        if (!usePrefer) {
            providerVecPtr = &requireVec;
        } else {
            providerVecPtr = &preferVec;
        }
    }
    const auto &providerVec = *providerVecPtr;
    if (providerVec.empty()) {
        return SearchServiceProviderPtr();
    }
    SearchServiceProviderPtr provider;
    if (HashPolicy::CONSISTENT_HASH == _hashPolicy) {
        provider = getProviderFromConsistHash(providerVec, key, param, probeProvider, type);
    } else {
        provider = getProviderFromRandomHash(providerVec, key, param, probeProvider, type);
    }
    if (!probeProvider) {
        probeProvider = getCopyProvider(matchTagMap, type);
    }
    if (!provider) {
        provider = getProviderFromAllByRandomHash(providerVec, key);
    }
    return provider;
}

void SearchServiceReplica::fillTagMaxVersion(const MatchTagMapPtr &matchTagMap) const {
    if (!matchTagMap) {
        return;
    }
    for (const auto &provider : _serviceVector) {
        provider->fillTagMaxVersion(*matchTagMap);
    }
}

bool SearchServiceReplica::getProviderVecByMatchTag(const MatchTagMapPtr &matchTagMap,
                                                    SearchServiceProviderVector &requireVec,
                                                    SearchServiceProviderVector &preferVec) {
    fillTagMaxVersion(matchTagMap);
    bool usePrefer = false;
    bool enableNull = (HashPolicy::CONSISTENT_HASH == _hashPolicy);
    for (const auto &provider : _serviceVector) {
        auto level = provider->matchTags(matchTagMap);
        if (TML_PREFER == level) {
            preferVec.push_back(provider);
            usePrefer = true;
        } else if (enableNull) {
            preferVec.push_back(SearchServiceProviderPtr());
        }
        if (TML_REQUIRE == level) {
            requireVec.push_back(provider);
        } else if (enableNull) {
            requireVec.push_back(SearchServiceProviderPtr());
        }
    }
    return usePrefer;
}

SearchServiceProviderPtr SearchServiceReplica::getProviderFromAllByRandomHash(
    const SearchServiceProviderVector &serviceVector, SourceIdTy key) {
    auto realIndex = getStartIndex(serviceVector, key);
    size_t vectorSize = serviceVector.size();
    SearchServiceProviderPtr requireProvider;
    for (auto i = 0; i < vectorSize; i++) {
        const auto &candidate = serviceVector[realIndex % vectorSize];
        if (candidate && candidate->getWeight() >= MIN_WEIGHT) {
            return candidate;
        }
        realIndex++;
    }
    return SearchServiceProviderPtr();
}

size_t SearchServiceReplica::getStartIndex(const SearchServiceProviderVector &serviceVector,
                                           SourceIdTy key) const {
    size_t vectorSize = serviceVector.size();
    std::vector<size_t> startedIndexVec(vectorSize);
    size_t startedCnt = 0;
    for (size_t i = 0; i < vectorSize; i++) {
        const auto &provider = serviceVector[i];
        if (!provider || provider->getWeight() < MIN_WEIGHT) {
            continue;
        }
        if (provider->isStarted()) {
            startedIndexVec[startedCnt] = i;
            startedCnt++;
        }
    }
    size_t realIndex = 0;
    if (startedCnt > 0 && startedCnt <= startedIndexVec.size()) {
        realIndex = startedIndexVec[key % startedCnt];
    } else {
        realIndex = key % vectorSize;
    }
    return realIndex;
}

SearchServiceProviderPtr SearchServiceReplica::getProviderFromConsistHash(
    const SearchServiceProviderVector &serviceVector, SourceIdTy key, const FlowControlParam &param,
    SearchServiceProviderPtr &probeProvider, RequestType &type) {
    auto it = _providerHashRing.get(key);
    if (it == _providerHashRing.end()) {
        return SearchServiceProviderPtr();
    }
    auto index = it->offset;
    assert(index < serviceVector.size());
    const auto &provider = serviceVector[index];
    if (provider) {
        auto weight = provider->getWeight();
        if (weight > MIN_WEIGHT) {
            if (weight >= it->label) {
                return provider;
            } else if (param.ignoreWeightLabelInConsistentHash) {
                AUTIL_LOG(DEBUG,
                          "biz [%s] ignored consistent hash provider [%s] weight [%d] < label [%d]",
                          _bizName.c_str(), provider->getSpec().ip.c_str(), weight, it->label);
                return provider;
            }
        }
        if (needProbe(provider, param.flowControlConfig, type)) {
            probeProvider = provider;
        }
    }
    auto nextProvider = findNextProviderInRing(serviceVector, it);
    if (nextProvider) {
        return nextProvider;
    } else if (provider && provider->isStarted() && provider->getWeight() >= MIN_WEIGHT) {
        // keep selecting consistent node is better than random
        return provider;
    }
    return SearchServiceProviderPtr();
}

SearchServiceProviderPtr SearchServiceReplica::getProviderFromRandomHash(
    const SearchServiceProviderVector &serviceVector, SourceIdTy key, const FlowControlParam &param,
    SearchServiceProviderPtr &probeProvider, RequestType &type) {
    auto probeRand = _randomGenerator.get();
    const auto &probeCandidate = serviceVector[probeRand % serviceVector.size()];
    if (probeCandidate && needProbe(probeCandidate, param.flowControlConfig, type)) {
        probeProvider = probeCandidate;
    }
    vector<RandomHashNode> weights;
    auto sum = getRandomHashWeights(serviceVector, weights);
    if (sum > 0) {
        auto index = RandomHash::get(key, weights);
        return serviceVector[index];
    } else {
        return SearchServiceProviderPtr();
    }
}

uint32_t
SearchServiceReplica::getRandomHashWeights(const SearchServiceProviderVector &serviceVector,
                                           vector<RandomHashNode> &weights) const {
    weights.reserve(serviceVector.size());
    uint32_t sum = 0;
    bool pureWeight = _hashPolicy == HashPolicy::RANDOM_HASH;
    for (size_t i = 0; i < serviceVector.size(); i++) {
        const auto &provider = serviceVector[i];
        if (!provider || !provider->isStarted()) {
            continue;
        }
        auto weight = pureWeight ? MAX_WEIGHT : provider->getWeight();
        if (weight > MIN_WEIGHT) {
            sum += weight;
            weights.emplace_back(sum, i);
        }
    }
    return sum;
}

bool SearchServiceReplica::needProbe(const SearchServiceProviderPtr &provider,
                                     const FlowControlConfigPtr &flowControlConfig,
                                     RequestType &type) {
    auto weight = provider->getWeight();
    if (weight > MIN_WEIGHT) {
        return false;
    }
    int32_t probePercent = 0;
    if (weight < 0) {
        // copy provider
        probePercent = weight;
    } else {
        probePercent = flowControlConfig->probePercent * MAX_WEIGHT_FLOAT;
    }
    return provider->needProbe(probePercent, type);
}

SearchServiceProviderPtr SearchServiceReplica::getCopyProvider(const MatchTagMapPtr &matchTagMap,
                                                               RequestType &type) {
    if (_serviceVectorCopy.empty()) {
        return SearchServiceProviderPtr();
    }
    auto normalProviderCount = _serviceVector.size();
    auto copyProviderCount = _serviceVectorCopy.size();
    auto totalProviderCount = normalProviderCount + copyProviderCount;

    auto index = _randomGenerator.get() % totalProviderCount;
    if (index < normalProviderCount) {
        return SearchServiceProviderPtr();
    }
    index = index - normalProviderCount;
    const auto &provider = _serviceVectorCopy[index];
    if (TML_NOT_MATCH == provider->matchTags(matchTagMap)) {
        return SearchServiceProviderPtr();
    }
    auto weight = provider->getWeight();
    assert(weight < 0);
    if (provider->needProbe(weight, type)) {
        return provider;
    } else {
        return SearchServiceProviderPtr();
    }
}

SearchServiceProviderPtr
SearchServiceReplica::findNextProviderInRing(const SearchServiceProviderVector &serviceVector,
                                             ConsistentHash::Iterator it) {
    auto orgIt = it++;
    uint32_t index = 0;
    set<uint32_t> tries;
    assert(!serviceVector.empty());
    auto count = serviceVector.size();
    while (orgIt != it && tries.size() < count) {
        if (it == _providerHashRing.end()) {
            it = _providerHashRing.begin();
            if (it == orgIt) {
                break;
            }
        }
        index = it->offset;
        assert(index < count);
        const auto &provider = serviceVector[index];
        if (provider) {
            auto weight = provider->getWeight();
            if (weight > it->label) {
                return provider;
            } else if (weight <= MIN_WEIGHT) {
                tries.insert(index);
            }
        } else {
            tries.insert(index);
        }
        it++;
    }
    return SearchServiceProviderPtr();
}

SearchServiceProviderPtr
SearchServiceReplica::getBackupProvider(const SearchServiceProviderPtr &selfProvider,
                                        SourceIdTy sourceId, const MatchTagMapPtr &matchTagMap,
                                        const FlowControlConfigPtr &flowControlConfig) {
    if (_serviceVector.empty()) {
        return SearchServiceProviderPtr();
    }
    {
        float baseAvgLatency = 0.0f;
        float baseErrorRatio = 0.0f;
        float percent = 1.0f;
        if (getDegradeCoverPercent(flowControlConfig, baseAvgLatency, baseErrorRatio, percent)) {
            if (percent < 0.9f) {
                AUTIL_INTERVAL_LOG(
                    200, WARN,
                    "degrade triggered, retry ignored, percent [%f] < 0.9f, baseAvgLatency[%fms], "
                    "beginDegradeLatency[%ums], fullDegradeLatency[%ums], baseErrorRatio[%f], "
                    "beginDegradeErrorRatio[%f], fullDegradeErrorRatio[%f], partId[%d], biz[%s]",
                    percent, baseAvgLatency, flowControlConfig->beginDegradeLatency,
                    flowControlConfig->fullDegradeLatency, baseErrorRatio,
                    flowControlConfig->beginDegradeErrorRatio,
                    flowControlConfig->fullDegradeErrorRatio, _partId, _bizName.c_str());
                return SearchServiceProviderPtr();
            }
        }
    }
    auto size = _serviceVector.size();
    auto beginIndex = sourceId;
    for (size_t i = 0; i < size; i++) {
        const auto &provider = _serviceVector[beginIndex % size];
        if (provider != selfProvider && provider->isHealth()) {
            if (TML_NOT_MATCH != provider->matchTags(matchTagMap)) {
                return provider;
            }
        }
        beginIndex++;
    }
    return SearchServiceProviderPtr();
}

ControllerChain *SearchServiceReplica::getBestChain() const {
    return _replicaController.getBestChain();
}

float SearchServiceReplica::getAvgWeight() const {
    return _replicaController.getAvgWeight();
}

WeightTy SearchServiceReplica::getReplicaWeight() {
    WeightTy weight = 0;
    for (const auto &provider : _serviceVector) {
        auto w = provider->getWeight();
        if (w > 0) {
            weight += w;
        }
    }
    return weight;
}

SearchServiceProviderPtr SearchServiceReplica::getProviderByAgentId(uint64_t agentId) {
    auto it = _providerMapByAgentId.find(agentId);
    if (_providerMapByAgentId.end() != it) {
        return it->second;
    }
    return SearchServiceProviderPtr();
}

void SearchServiceReplica::fillPropagationStat(uint64_t skipAgentId, google::protobuf::Arena *arena,
                                               PropagationStats &propagationStats) {
    auto propagationStatDef = google::protobuf::Arena::CreateMessage<PropagationStatDef>(arena);
    SearchServiceProviderPtr beginProvider;
    auto randomNum = getRandom();
    int payloadSize = PAYLOAD_SIZE;
    for (int payLoadCount = 0; payLoadCount < payloadSize;) {
        auto p = getProviderByIndex(randomNum++);
        if (!p) {
            break;
        }
        if (!beginProvider) {
            beginProvider = p;
        } else if (beginProvider == p) {
            break;
        }
        if (skipAgentId == p->getAgentId()) {
            continue;
        }
        if (!doFillPropagationStat(p, *propagationStatDef)) {
            continue;
        }
        propagationStats.add_stats()->CopyFrom(*propagationStatDef);
        payLoadCount++;
    }
    freeProtoMessage(propagationStatDef);
}

bool SearchServiceReplica::doFillPropagationStat(const SearchServiceProviderPtr &provider,
                                                 PropagationStatDef &propagationStat) {
    const auto &latencyController = provider->getControllerChain()->latencyController;
    if (!latencyController.isValid()) {
        return false;
    }
    auto version = latencyController.serverValueVersion();
    if (unlikely(0 == version)) {
        // compatible logic
        return false;
    }
    auto avgLatency = propagationStat.mutable_latency();
    avgLatency->set_version(version);
    avgLatency->set_agent_id(provider->getAgentId());
    avgLatency->set_latency(latencyController.serverValue());
    avgLatency->set_load_balance_latency(latencyController.loadBalanceServerValue());
    avgLatency->set_avg_weight(latencyController.serverAvgWeight());

    const auto &errorRatioController = provider->getControllerChain()->errorRatioController;
    auto errorRatio = propagationStat.mutable_error();
    errorRatio->set_ratio(errorRatioController.serverValue());
    errorRatio->set_load_balance_ratio(errorRatioController.loadBalanceServerValue());

    const auto &degradeRatioController = provider->getControllerChain()->degradeRatioController;
    auto degradeRatio = propagationStat.mutable_degrade();
    degradeRatio->set_ratio(degradeRatioController.serverValue());
    degradeRatio->set_load_balance_ratio(degradeRatioController.loadBalanceServerValue());

    return INVALID_FILTER_VALUE != errorRatio->ratio() &&
           INVALID_FILTER_VALUE != errorRatio->load_balance_ratio() &&
           INVALID_FILTER_VALUE != degradeRatio->ratio() &&
           INVALID_FILTER_VALUE != degradeRatio->load_balance_ratio() &&
           INVALID_FILTER_VALUE != avgLatency->latency() &&
           INVALID_FILTER_VALUE != avgLatency->load_balance_latency();
}

SearchServiceProviderPtr SearchServiceReplica::getProviderByIndex(uint64_t index) {
    if (_serviceVector.empty()) {
        return SearchServiceProviderPtr();
    }
    index = index % _serviceVector.size();
    return _serviceVector[index];
}

bool SearchServiceReplica::hasHealthProvider() const {
    for (const auto &provider : _serviceVector) {
        if (provider->isHealth()) {
            return true;
        }
    }
    return false;
}

size_t SearchServiceReplica::getHeartbeatHealthCount() const {
    size_t count = 0;
    for (const auto &provider : _serviceVector) {
        if (provider->isHeartbeatHealth()) {
            ++count;
        }
    }
    return count;
}

bool SearchServiceReplica::hasStartedProvider() const {
    for (const auto &provider : _serviceVector) {
        if (provider->isStarted()) {
            return true;
        }
    }
    return false;
}

bool SearchServiceReplica::needDegrade(uint64_t key, const FlowControlParam &param,
                                       SearchServiceProviderPtr &probeProvider, RequestType &type) {
    float baseAvgLatency = 0.0f;
    float baseErrorRatio = 0.0f;
    float percent = 1.0f;
    const auto &flowControlConfig = param.flowControlConfig;
    if (param.disableDegrade) {
        return false;
    }
    if (!getDegradeCoverPercent(flowControlConfig, baseAvgLatency, baseErrorRatio, percent)) {
        return false;
    }
    bool ret = false;
    float minDegradePercent = MAX_PROBE_PERCENT / 100.0f;
    if (percent == 0.0f) {
        // full degrade need probe
        const auto &provider = _serviceVector[key % _serviceVector.size()];
        if (provider->needProbe(flowControlConfig->probePercent * MAX_WEIGHT_FLOAT, type)) {
            probeProvider = provider;
        }
        ret = true;
    } else if (percent < minDegradePercent) {
        percent = minDegradePercent;
    }
    if (ret || needDegrade(key, param, percent)) {
        AUTIL_INTERVAL_LOG(200, WARN,
                           "multi_call degrade triggered, percent [%f], "
                           "baseAvgLatency[%fms], beginDegradeLatency[%ums], "
                           "fullDegradeLatency[%ums], baseErrorRatio[%f], "
                           "beginDegradeErrorRatio[%f], fullDegradeErrorRatio[%f], "
                           "partId[%d], biz[%s]",
                           percent, baseAvgLatency, flowControlConfig->beginDegradeLatency,
                           flowControlConfig->fullDegradeLatency, baseErrorRatio,
                           flowControlConfig->beginDegradeErrorRatio,
                           flowControlConfig->fullDegradeErrorRatio, _partId, _bizName.c_str());
        return true;
    } else {
        return false;
    }
}

bool SearchServiceReplica::getDegradeCoverPercent(const FlowControlConfigPtr &flowControlConfig,
                                                  float &baseAvgLatency, float &baseErrorRatio,
                                                  float &percent) {
    float latencyPercent = 1.0f;
    float errorPercent = 1.0f;
    if (!getLatencyCoverPercent(flowControlConfig, baseAvgLatency, latencyPercent)) {
        return false;
    }
    if (!getErrorRatioCoverPercent(flowControlConfig, baseErrorRatio, errorPercent)) {
        return false;
    }
    percent = min(latencyPercent, errorPercent);
    return true;
}

bool SearchServiceReplica::getLatencyCoverPercent(const FlowControlConfigPtr &flowControlConfig,
                                                  float &baseAvgLatency, float &percent) {
    if (!_replicaController.getBaseAvgLatency(baseAvgLatency)) {
        // average latency not available
        return false;
    }
    auto beginDegradeLatency = flowControlConfig->beginDegradeLatency;
    auto fullDegradeLatency = flowControlConfig->fullDegradeLatency;
    if (baseAvgLatency < beginDegradeLatency) {
        percent = 1.0f;
        return true;
    }
    uint32_t range = fullDegradeLatency - beginDegradeLatency;
    if (0 == range) {
        percent = baseAvgLatency > beginDegradeLatency ? 0.0f : 1.0f;
    } else {
        percent = max(0.0f, 1.0f - (baseAvgLatency - beginDegradeLatency) / range);
    }
    return true;
}

bool SearchServiceReplica::getErrorRatioCoverPercent(const FlowControlConfigPtr &flowControlConfig,
                                                     float &baseErrorRatio, float &percent) {
    if (!_replicaController.getBaseErrorRatio(baseErrorRatio)) {
        // average error ratio not available
        return false;
    }
    baseErrorRatio /= MAX_RATIO;
    auto beginDegradeErrorRatio = flowControlConfig->beginDegradeErrorRatio;
    auto fullDegradeErrorRatio = flowControlConfig->fullDegradeErrorRatio;
    if (baseErrorRatio < beginDegradeErrorRatio) {
        percent = 1.0f;
        return true;
    }
    auto range = fullDegradeErrorRatio - beginDegradeErrorRatio;
    if (range <= EPSILON) {
        percent = baseErrorRatio > beginDegradeErrorRatio ? 0.0f : 1.0f;
    } else {
        percent = max(0.0f, 1.0f - (baseErrorRatio - beginDegradeErrorRatio) / range);
    }
    return true;
}

bool SearchServiceReplica::needDegrade(uint64_t key, const FlowControlParam &param, float percent) {
    uint32_t totalRange = param.partitionCount * MAX_WEIGHT;
    uint32_t from = param.partitionIndex * MAX_WEIGHT;
    uint32_t to = from + totalRange * percent;
    assert(totalRange > 0);
    uint32_t s = key % totalRange;
    if (s >= from && s < to) {
        return false;
    }
    return (to < totalRange) || s >= (to - totalRange);
}

void SearchServiceReplica::fillReplicaInfo(SnapshotBizInfo &bizInfo) const {
    bizInfo.providerCount += _serviceVector.size();
    for (const auto &ptr : _serviceVector) {
        const auto &provider = *ptr;
        if (provider.isHealth() || provider.getTotalQueryCounter() == 0) {
            bizInfo.healthProviderCount++;
        } else if (provider.getWeight() < MIN_WEIGHT) {
            bizInfo.copyProviderCount++;
        } else {
            bizInfo.unhealthProviderCount++;
        }
        auto serverValueDelayMs = provider.serverValueDelayMs();
        if (INVALID_FILTER_VALUE != serverValueDelayMs) {
            bizInfo.delaySum += serverValueDelayMs;
            bizInfo.delayCount++;
        }
    }
    _replicaController.fillControllerInfo(bizInfo);
}

FlowControlConfigPtr SearchServiceReplica::getLastFlowControlConfig() const {
    autil::ScopedReadWriteLock lock(_lastFlowControlConfigLock, 'r');
    return _lastFlowControlConfig;
}

void SearchServiceReplica::trySetLastFlowControlConfig(const FlowControlConfigPtr &config) {
    if (0 == _lastFlowControlConfigLock.trywrlock()) {
        _lastFlowControlConfig = config;
        _lastFlowControlConfigLock.unlock();
    }
}

MetaMapPtr SearchServiceReplica::getOneMeta() const {
    for (const auto &provider : _serviceVector) {
        auto metas = provider->getHeartbeatMetas();
        if (metas) {
            return metas;
        }
    }
    return nullptr;
}

void SearchServiceReplica::toString(std::string &debugStr) {
    _replicaController.toString(debugStr);
    debugStr += ", lackRatio: ";
    debugStr += StringUtil::fToString(_degradeRatio.output());
    debugStr += " %, " + _queryCounter.snapshotStr();
    debugStr += "\n";
    auto bestChain = _replicaController.getBestChain();
    for (size_t i = 0; i < _serviceVector.size(); ++i) {
        debugStr += "      ";
        _serviceVector[i]->toString(bestChain, debugStr);
    }
    for (size_t i = 0; i < _serviceVectorCopy.size(); ++i) {
        debugStr += "      ";
        _serviceVectorCopy[i]->toString(bestChain, debugStr);
    }
    debugStr += "\n";
}

} // namespace multi_call
