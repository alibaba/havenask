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
#include "aios/network/gig/multi_call/service/SearchServiceProvider.h"

#include <math.h>

#include "aios/network/gig/multi_call/new_heartbeat/ClientTopoInfoMap.h"
#include "aios/network/gig/multi_call/new_heartbeat/HostHeartbeatInfo.h"
#include "aios/network/gig/multi_call/stream/GigStreamRequest.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, SearchServiceProvider);

bool operator==(const SearchServiceProvider &lhs, const SearchServiceProvider &rhs) {
    return lhs.getNodeId() == rhs.getNodeId();
}

SearchServiceProvider::SearchServiceProvider(const ConnectionManagerPtr &connectionManager,
                                             const TopoNode &topoNode)
    : _randomGenerator(autil::TimeUtility::currentTimeInMicroSeconds())
    , _clockDrift(200)
    , _weightBonus(WEIGHT_DEC_LIMIT)
    , _useDefaultProbePercent(false)
    , _loadBalanceWeightFactor(MAX_PERCENT)
    , _connectionManager(connectionManager)
    , _spec(topoNode.spec)
    , _nodeId(topoNode.nodeId)
    , _clusterName(topoNode.clusterName)
    , _protocalVersion(topoNode.protocalVersion)
    , _isCopy(false)
    , _isValid(false)
    , _ssType(topoNode.ssType) {
    _agentId = HashAlgorithm::hashString64(_nodeId.c_str(), _nodeId.size());

    atomic_set(&_previousServerVersion, 0);
    _nodeMeta = topoNode.meta;
    _targetMetaEnv = topoNode.meta.metaEnv;
    _hbInfo = topoNode.hbInfo;
    auto realWeight = topoNode.getNodeWeight();
    if (likely(realWeight >= MIN_WEIGHT)) {
        // normal provider
        _weight.f = MIN_WEIGHT_FLOAT;
        _loadBalanceWeightFactor = 0.0f;
    } else {
        // copy provider
        _weight.f = realWeight;
        _loadBalanceWeightFactor = -1.0f * MAX_PERCENT;
        setCopy(true);
    }
    updateValidState(topoNode.isValid);
    updateCachedWeight();
    _subscribeTime = getCurrentTimeStrUs();
    _clockDrift.setMinValue(-INVALID_FILTER_VALUE);
}

SearchServiceProvider::~SearchServiceProvider() {
}

const HbInfoPtr &SearchServiceProvider::getHeartbeatInfo() const {
    return _hbInfo;
}

bool SearchServiceProvider::supportHeartbeat() const {
    return _hbInfo && MC_PROTOCOL_UNKNOWN != _hbInfo->heartbeatProtocol;
}

ProtocolType SearchServiceProvider::getHeartbeatProtocol() const {
    return _hbInfo ? _hbInfo->heartbeatProtocol : MC_PROTOCOL_UNKNOWN;
}

std::string SearchServiceProvider::getHeartbeatSpec() const {
    return _hbInfo ? _hbInfo->heartbeatSpec : "";
}

void SearchServiceProvider::updateHeartbeatInfo(const HbMetaInfoPtr &hbMetaPtr) {
    if (_hbInfo) {
        _hbInfo->updateHbMeta(hbMetaPtr);
    }
}

void SearchServiceProvider::updateHeartbeatTime(int64_t time) {
    if (_hbInfo) {
        _hbInfo->updateTime(time);
    }
}

bool SearchServiceProvider::isHeartbeatHealth() const {
    if (!_hbInfo) {
        return false;
    }
    int64_t now = TimeUtility::currentTime();
    if (now - _hbInfo->getTime() > ControllerParam::HEARTBEAT_UNHEALTH_INTERVAL) {
        return false;
    }
    return true;
}

void SearchServiceProvider::collectSpecs(set<string> &keepSpecs) const {
    ScopedReadWriteLock lock(_connectionLock, 'r');
    for (uint32_t i = MC_PROTOCOL_TCP; i < MC_PROTOCOL_UNKNOWN; i++) {
        const auto &con = _connection[i];
        if (con) {
            keepSpecs.insert(con->getSpec());
        }
    }
}

ConnectionPtr SearchServiceProvider::getConnection(ProtocolType type) {
    if (type >= MC_PROTOCOL_UNKNOWN) {
        return ConnectionPtr();
    }
    ConnectionPtr con;
    {
        ScopedReadWriteLock lock(_connectionLock, 'r');
        con = _connection[type];
    }
    if (type != MC_PROTOCOL_GRPC_STREAM && INVALID_PORT == _spec.ports[type]) {
        return ConnectionPtr();
    }
    if (con && con->getType() != type) {
        AUTIL_LOG(ERROR,
                  "attempt to use two type of protocol in one biz"
                  ", previous [%s], current [%s], spec[%s]",
                  convertProtocolTypeToStr(con->getType()).c_str(),
                  convertProtocolTypeToStr(type).c_str(), getAnetSpec(type).c_str());
        return ConnectionPtr();
    }
    if (!con) {
        const auto &spec = getAnetSpec(type);
        con = _connectionManager->getConnection(spec, type);
        ScopedReadWriteLock lock(_connectionLock, 'w');
        _connection[type] = con;
    }
    return con;
}

bool SearchServiceProvider::needProbe(WeightTy probePercent, RequestType &type) {
    WeightTy rand = _randomGenerator.get() % MAX_WEIGHT;
    if (unlikely(probePercent < 0)) {
        type = RT_COPY;
        return rand < -probePercent;
    }
    bool stopped = _controllerChain.targetWeight == MIN_WEIGHT;
    if (_useDefaultProbePercent || stopped) {
        // use default when init probe finished or provider is stopped
        probePercent = MAX_PROBE_PERCENT;
    }
    if (_controllerChain.warmUpController.needProbe()) {
        probePercent = MAX_WEIGHT;
    }
    auto controllerNeedProbe = _controllerChain.latencyController.needProbe() ||
                               _controllerChain.errorRatioController.needProbe() ||
                               _controllerChain.degradeRatioController.needProbe();
    if (rand < MAX_PROBE_PERCENT || (!stopped && controllerNeedProbe)) {
        // probe more aggressively if new start
        type = RT_PROBE;
        return true;
    } else if (rand < probePercent) {
        type = RT_COPY;
        return true;
    }
    return false;
}

void SearchServiceProvider::setWeight(float weight) {
    _loadBalanceWeightFactor = weight / MAX_WEIGHT_FLOAT;
    _weight.f = weight;
    updateCachedWeight();
}

void SearchServiceProvider::updateWeight(ControllerFeedBack &feedBack) {
    if (unlikely(isCopy())) {
        // copy provider
        return;
    }
    auto diff = 0.0f;
    _controllerChain.degradeRatioController.update(feedBack);
    diff = min(diff, _controllerChain.latencyController.update(feedBack));
    diff = min(diff, _controllerChain.errorRatioController.update(feedBack));
    diff = min(diff, _controllerChain.errorController.update(feedBack));
    diff = min(diff, _controllerChain.warmUpController.update(feedBack));
    updateLoadBalance(feedBack, false);
    auto hasError = feedBack.stat.isFailed();
    auto newTargetWeight = min(_nodeMeta.targetWeight, feedBack.stat.targetWeight);
    if (hasError) {
        auto currentTargetWeight = _controllerChain.targetWeight;
        newTargetWeight = min(newTargetWeight, currentTargetWeight);
    }
    auto diff2 = approachTargetWeight(newTargetWeight);
    if (diff >= 0.0f) {
        diff = diff2;
    } else {
        diff = min(diff, diff2);
    }
    diff = diff * feedBack.maxWeight / MAX_WEIGHT_FLOAT;
    if (hasError) {
        diff = min(diff, 0.0f);
    }
    doUpdateWeight(diff, feedBack.minWeight);
}

void SearchServiceProvider::updateLoadBalance(ControllerFeedBack &feedBack, bool updateWeight) {
    if (unlikely(isCopy())) {
        // copy provider
        return;
    }
    auto &latencyController = _controllerChain.latencyController;
    auto &degradeRatioController = _controllerChain.degradeRatioController;
    auto &errorRatioController = _controllerChain.errorRatioController;
    float newFactor = MAX_PERCENT;

    errorRatioController.updateLoadBalance(feedBack);
    newFactor = min(newFactor, degradeRatioController.updateLoadBalance(feedBack));
    newFactor = min(newFactor, latencyController.updateLoadBalance(feedBack));

    updateWeightFactor(latencyController.serverValueVersion(), newFactor);
    if (updateWeight) {
        updateCachedWeight(feedBack.minWeight);
    }
}

void SearchServiceProvider::updateWeightFactor(int64_t version, float newFactor) {
    AUTIL_LOG(DEBUG, "update new factor, node: %s, factor: %f, version: %ld", _nodeId.c_str(),
              newFactor, version);
    if (unlikely(0 == version)) {
        // compatible logic
        _loadBalanceWeightFactor = std::min(1.0f, std::max(0.0f, newFactor));
        return;
    }
    float newValue = _loadBalanceWeightFactor;
    int64_t previousVersion = 0;
    do {
        previousVersion = atomic_read(&_previousServerVersion);
        float oldFactor = _loadBalanceWeightFactor;
        if (version <= previousVersion) {
            return;
        }
        int64_t timeDiff = version - previousVersion;
        auto diffRange = std::min(1.0f, timeDiff / FACTOR_US_TO_MS / LOAD_BALANCE_TIME_FACTOR_MS);
        float diff = newFactor - oldFactor;
        if (diff < 0.0f) {
            diff = std::max(-1.0f * diffRange, diff);
        }
        newValue = oldFactor + diff;
    } while (!compare_and_swap(&_previousServerVersion, previousVersion, version));
    _loadBalanceWeightFactor = std::max(0.0f, newValue);
}

void SearchServiceProvider::updateCachedWeight(float minWeight) {
    auto weightFloat = _weight.f;
    auto loadBalanceWeightFactor = _loadBalanceWeightFactor;
    if (unlikely(isCopy())) {
        if (weightFloat < MIN_WEIGHT_FLOAT && loadBalanceWeightFactor < MIN_PERCENT) {
            _weightCached = min(-1.0f, ceilf(weightFloat));
        }
        return;
    }
    if (weightFloat < MIN_WEIGHT_FLOAT || loadBalanceWeightFactor < MIN_PERCENT) {
        return;
    }
    float newWeight = 0.0f;
    if (hasServerAgent()) {
        if (weightFloat > MIN_WEIGHT_FLOAT) {
            newWeight = ceilf(MAX_WEIGHT_FLOAT * loadBalanceWeightFactor);
        } else {
            newWeight = MIN_WEIGHT_FLOAT;
        }
    } else {
        newWeight = ceilf(weightFloat);
    }
    newWeight = std::max(MIN_WEIGHT_FLOAT, std::min(MAX_WEIGHT_FLOAT, newWeight));
    if (newWeight < WEIGHT_DEC_LIMIT && newWeight > MIN_WEIGHT_FLOAT) {
        newWeight = WEIGHT_DEC_LIMIT;
    }
    WeightTy targetWeight = getTargetWeight();
    _weightCached = std::max((WeightTy)minWeight, std::min(targetWeight, (WeightTy)newWeight));
    AUTIL_LOG(DEBUG,
              "update cached weight, node: %s, newWeight: %f, hasServer: %d, "
              "_weightCached: %d",
              _nodeId.c_str(), newWeight, hasServerAgent(), _weightCached);
}

WeightTy SearchServiceProvider::getWeight() const {
    return _weightCached;
}

WeightTy SearchServiceProvider::getTargetWeight() const {
    WeightTy agentTargetWeight = _controllerChain.targetWeight;
    return std::min(agentTargetWeight, _nodeMeta.targetWeight);
}

void SearchServiceProvider::setTargetWeight(WeightTy targetWeight) {
    if (unlikely(isCopy())) {
        if (targetWeight >= 0) {
            return;
        }
        targetWeight = std::max(-MAX_WEIGHT, targetWeight);
        setWeight(targetWeight);
    } else if (targetWeight > MAX_WEIGHT) {
        targetWeight = MAX_WEIGHT;
    } else if (targetWeight < MIN_WEIGHT) {
        targetWeight = MIN_WEIGHT;
    }
    _nodeMeta.targetWeight = targetWeight;
    _controllerChain.targetWeight = targetWeight;
    updateCachedWeight();
}

bool SearchServiceProvider::isBetterThan(const ControllerChain *thisChain,
                                         const ControllerChain *bestChain,
                                         const MetricLimits &metricLimits) {
    assert(thisChain);
    if (!bestChain) {
        return true;
    }
    if (thisChain == bestChain) {
        return false;
    }
    if (MIN_WEIGHT == thisChain->targetWeight) {
        return false;
    } else if (MIN_WEIGHT == bestChain->targetWeight) {
        return true;
    }
    int32_t thisOk = isLegal(thisChain, bestChain, metricLimits);
    int32_t bestOk = isLegal(bestChain, thisChain, metricLimits);
    // cout << "thisOk: " << thisOk << ", bestOk: " << bestOk << endl;
    int32_t stat = thisOk | (bestOk << 1u);
    switch (stat) {
    case 0:
        // both illegal, error first
        return thisIsBest(*thisChain, *bestChain, true);
    case 3:
        // both ok means error is ok, latency first
        return thisIsBest(*thisChain, *bestChain, false);
    case 1:
        return true;
    case 2:
        return false;
    default:
        return false;
    }
}

bool SearchServiceProvider::isLegal(const ControllerChain *candidateChain,
                                    const ControllerChain *baseChain,
                                    const MetricLimits &metricLimits) {
    assert(candidateChain);
    assert(baseChain);
    auto newMetricLimits = metricLimits;
    newMetricLimits.errorRatioLimit =
        std::min(ERROR_RATIO_TOLERANCE / MAX_RATIO, newMetricLimits.errorRatioLimit);
    return candidateChain->degradeRatioController.isLegal(baseChain, newMetricLimits) &&
           candidateChain->latencyController.isLegal(baseChain, newMetricLimits) &&
           candidateChain->errorRatioController.isLegal(baseChain, newMetricLimits);
}

bool SearchServiceProvider::thisIsBest(const ControllerChain &thisChain,
                                       const ControllerChain &otherChain, bool errorFirst) {
    auto errorBetter = thisChain.errorRatioController.compare(otherChain.errorRatioController,
                                                              ERROR_RATIO_TOLERANCE);
    if (errorFirst && errorBetter.isValid()) {
        return errorBetter;
    }
    auto degradeBetter = thisChain.degradeRatioController.compare(otherChain.degradeRatioController,
                                                                  DEGRADE_RATIO_TOLERANCE);
    if (degradeBetter.isValid()) {
        return degradeBetter;
    }
    auto latencyBetter = thisChain.latencyController.compare(otherChain.latencyController);
    if (latencyBetter.isValid()) {
        return latencyBetter;
    }
    if (errorBetter.isValid()) {
        return errorBetter;
    }
    return false;
}

float SearchServiceProvider::approachTargetWeight(float targetWeight) {
    _controllerChain.targetWeight = targetWeight;
    if (MIN_WEIGHT == targetWeight) {
        // stopped, use user probe percent when restarted
        _useDefaultProbePercent = false;
    }
    float step =
        (ControllerParam::WEIGHT_UPDATE_STEP + _weightBonus) * targetWeight / MAX_WEIGHT_FLOAT;
    auto diff = 0.0f;
    float currentWeight = _weight.f;
    if (targetWeight > currentWeight) {
        diff = min(targetWeight - currentWeight, step);
    } else if (targetWeight < currentWeight) {
        diff = targetWeight - currentWeight;
    }
    return diff;
}

void SearchServiceProvider::doUpdateWeight(float diff, float minWeight) {
    updateWeightBonus(diff);
    FloatIntUnion current;
    FloatIntUnion newWeight;
    do {
        current.f = _weight.f;
        newWeight.f = current.f + diff;
        if (newWeight.f > MAX_WEIGHT_FLOAT) {
            newWeight.f = MAX_WEIGHT_FLOAT;
        } else if (newWeight.f < MIN_WEIGHT_FLOAT) {
            newWeight.f = MIN_WEIGHT_FLOAT;
        }
    } while (!cmpxchg(&_weight.i, current.i, newWeight.i));
    updateCachedWeight(minWeight);
    if (diff > 0.0f && controllerValid()) {
        // init probe finished
        _useDefaultProbePercent = true;
    }
}

bool SearchServiceProvider::controllerValid() const {
    return _controllerChain.latencyController.isValid() &&
           _controllerChain.errorRatioController.isValid() &&
           _controllerChain.degradeRatioController.isValid();
}

void SearchServiceProvider::updateWeightBonus(float diff) {
    const auto &degradeRatioController = _controllerChain.degradeRatioController;
    auto degrading = degradeRatioController.isValid() &&
                     degradeRatioController.controllerOutput() > DEGRADE_RATIO_TOLERANCE;
    auto newBonus = 0.0f;
    if (diff < 0.0f || degrading) {
        newBonus = 0.0f;
    } else {
        newBonus = _weightBonus + MIN_WEIGHT_UPDATE_STEP_FLOAT;
    }
    if (newBonus > WEIGHT_DEC_LIMIT) {
        newBonus = WEIGHT_DEC_LIMIT;
    }
    _weightBonus = newBonus;
}

void SearchServiceProvider::updateValidState(bool isValid) {
    if (unlikely(isCopy())) {
        return;
    }
    if (_isValid && !isValid) {
        setWeight(0.0f);
    }
    _isValid = isValid;
}

bool SearchServiceProvider::isHealth() const {
    return getWeight() > MIN_WEIGHT;
}

bool SearchServiceProvider::isStarted() const {
    return _controllerChain.targetWeight > MIN_WEIGHT;
}

void SearchServiceProvider::updateTagsFromMap(const TagMap &tags) {
    auto oldTagMap = getTags();
    auto tagMapPtr = std::make_shared<TagInfoMap>();
    for (const auto &pair : tags) {
        const auto &tag = pair.first;
        auto version = pair.second;
        bool newTag = true;
        TagInfo info;
        if (oldTagMap) {
            auto oldIt = oldTagMap->find(tag);
            if (oldTagMap->end() != oldIt) {
                const auto &oldTagInfo = oldIt->second;
                if (version == oldTagInfo.version) {
                    info = oldTagInfo;
                    newTag = false;
                }
            }
        }
        if (newTag) {
            info.version = version;
            info.date = getCurrentTimeStrUs();
        }
        tagMapPtr->emplace(tag, info);
    }
    if (tagMapPtr->empty()) {
        tagMapPtr.reset();
    }
    updateTags(tagMapPtr);
}

void SearchServiceProvider::updateTags(const TagInfoMapPtr &tags) {
    ScopedReadWriteLock lock(_heartbeatLock, 'w');
    _tags = tags;
}

TagInfoMapPtr SearchServiceProvider::getTags() const {
    ScopedReadWriteLock lock(_heartbeatLock, 'r');
    return _tags;
}

void SearchServiceProvider::updateHeartbeatMetas(const MetaMapPtr &metas) {
    ScopedReadWriteLock lock(_heartbeatLock, 'w');
    _metas = metas;
}

MetaMapPtr SearchServiceProvider::getHeartbeatMetas() const {
    ScopedReadWriteLock lock(_heartbeatLock, 'r');
    return _metas;
}

void SearchServiceProvider::fillTagMaxVersion(MatchTagMap &matchTagMap) {
    auto tags = getTags();
    if (!tags) {
        return;
    }
    const auto &thisTagMap = *tags;
    for (auto &pair : matchTagMap) {
        const auto &tag = pair.first;
        auto &matchInfo = pair.second;
        auto it = thisTagMap.find(tag);
        if (thisTagMap.end() == it) {
            continue;
        }
        auto thisVersion = it->second.version;
        if (thisVersion > matchInfo.version) {
            matchInfo.version = thisVersion;
        }
    }
}

TagMatchLevel SearchServiceProvider::matchTags(const MatchTagMapPtr &matchTagMap) const {
    if (!matchTagMap) {
        return TML_PREFER;
    }
    auto tags = getTags();
    const auto &matchMap = *matchTagMap;
    bool preferMatched = true;
    for (const auto &pair : matchMap) {
        const auto &tag = pair.first;
        const auto &matchInfo = pair.second;
        bool matched = false;
        if (tags) {
            auto it = tags->find(tag);
            if (tags->end() != it) {
                if (it->second.version >= matchInfo.version) {
                    matched = true;
                }
            }
        }
        if (!matched) {
            if (TMT_REQUIRE == matchInfo.type) {
                return TML_NOT_MATCH;
            } else if (TMT_PREFER == matchInfo.type) {
                preferMatched = false;
            }
        }
    }
    if (preferMatched) {
        return TML_PREFER;
    } else {
        return TML_REQUIRE;
    }
}

std::string SearchServiceProvider::getCurrentTimeStrUs() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return getTimeStrUs(time.tv_sec, time.tv_usec);
}

std::string SearchServiceProvider::getTimeStrUs(time_t second, int64_t usec) {
    struct tm tim;
    ::localtime_r(&second, &tim);

    constexpr int32_t timeLen = 48;
    char buffer[timeLen];
    snprintf(buffer, timeLen, "%04d-%02d-%02d_%02d:%02d:%02d.%06ld", tim.tm_year + 1900,
             tim.tm_mon + 1, tim.tm_mday, tim.tm_hour, tim.tm_min, tim.tm_sec, usec);
    return std::string(buffer);
}

std::string SearchServiceProvider::getTagVersionStr(int64_t version, int64_t currentTime) {
    auto ret = autil::StringUtil::toString(version);
    // diff less than one year, parse version as time
    if (abs(currentTime - version) < (365 * 24 * 3600 * 1000000L)) {
        auto second = version / 1000000;
        auto usec = version % 1000000;
        ret += "(" + getTimeStrUs(second, usec) + ")";
    }
    return ret;
}

bool SearchServiceProvider::post(const RequestPtr &request, const CallBackPtr &callBack) {
    ConnectionPtr con = getConnection(request->getProtocolType());
    if (con) {
        con->post(request, callBack);
        return true;
    } else {
        callBack->run(NULL, MULTI_CALL_REPLY_ERROR_CONNECTION, string(), string());
        return false;
    }
}

void SearchServiceProvider::updateRequestCounter(const CallBack *callBack) {
    if (callBack->isRetry()) {
        _totalRetryCounter.inc();
        return;
    }
    auto requestType = callBack->getRequestType();
    switch (requestType) {
    case RT_NORMAL:
        _totalNormalCounter.inc();
        break;
    case RT_PROBE:
        _totalProbeCounter.inc();
        break;
    case RT_COPY:
        _totalCopyCounter.inc();
        break;
    default:
        break;
    }
}

void SearchServiceProvider::updateResponseCounter() {
    auto hostStats = getHostStats();
    if (hostStats) {
        hostStats->updateLastResponseTime();
    }
}

void SearchServiceProvider::setHostStats(const std::shared_ptr<HostHeartbeatStats> &hostStats) {
    ScopedReadWriteLock lock(_heartbeatLock, 'w');
    _hostStats = hostStats;
}

void SearchServiceProvider::incStreamQueryCount() {
    _totalStreamQueryCounter.inc();
}

void SearchServiceProvider::incStreamResponseCount() {
    updateResponseCounter();
    _totalStreamResponseCounter.inc();
}

std::string SearchServiceProvider::getAnetSpec(ProtocolType type) {
    return _spec.getAnetSpec(type);
}

std::string SearchServiceProvider::getSpecStr(ProtocolType type) {
    return _spec.getSpecStr(type);
}

void SearchServiceProvider::toString(const ControllerChain *bestChain,
                                     std::string &debugStr) const {
    debugStr += "cluster: " + _clusterName + ", node: " + _nodeId +
                ", sType:" + convertSubscribeTypeToStr(_ssType);
    if (bestChain == &_controllerChain) {
        debugStr += ", *** ";
    } else {
        debugStr += ",     ";
    }
    debugStr += "weight: ";
    debugStr += StringUtil::toString(getWeight());
    debugStr += " = ";
    debugStr += StringUtil::fToString(_weight.f);
    debugStr += " * ";
    debugStr += StringUtil::fToString(_loadBalanceWeightFactor);
    debugStr += ", ";
    _controllerChain.toString(debugStr);
    debugStr += ", totalNormalCounter: ";
    debugStr += _totalNormalCounter.snapshotStr();

    debugStr += ", totalRetryCounter: ";
    debugStr += _totalRetryCounter.snapshotStr();

    debugStr += ", totalProbeCounter: ";
    debugStr += _totalProbeCounter.snapshotStr();

    debugStr += ", totalCopyCounter: ";
    debugStr += _totalCopyCounter.snapshotStr();

    debugStr += ", totalStreamQueryCounter: ";
    debugStr += _totalStreamQueryCounter.snapshotStr();

    debugStr += ", totalStreamResponseCounter: ";
    debugStr += _totalStreamResponseCounter.snapshotStr();

    float drift = 0.0f;
    if (_clockDrift.isValid()) {
        drift = _clockDrift.output();
    }
    debugStr += ", clockDrift: ";
    debugStr += autil::StringUtil::fToString(drift);

    debugStr += ", bonus: ";
    debugStr += StringUtil::fToString(_weightBonus);

    debugStr += ", ";
    debugStr += _subscribeTime;
    auto tags = getTags();
    if (tags) {
        debugStr += ", tags:";
        auto currentTime = TimeUtility::currentTime();
        for (const auto &pair : *tags) {
            const auto &tag = pair.first;
            const auto &tagInfo = pair.second;
            debugStr += " " + tag + "(" + getTagVersionStr(tagInfo.version, currentTime) + " " +
                        autil::StringUtil::toString(tagInfo.date) + ")";
        }
    }
    debugStr += "\n";
}

void SearchServiceProvider::updateStaticParam(const MiscConfigPtr &miscConfig) {
    float step = miscConfig->weightUpdateStep;
    if (step < MIN_WEIGHT_UPDATE_STEP_FLOAT) {
        step = MIN_WEIGHT_UPDATE_STEP_FLOAT;
    } else if (step > MAX_WEIGHT_UPDATE_STEP_FLOAT) {
        step = MAX_WEIGHT_UPDATE_STEP_FLOAT;
    }
    ControllerParam::WEIGHT_UPDATE_STEP = step;

    ControllerParam::HEARTBEAT_UNHEALTH_INTERVAL =
        miscConfig->heartbeatUnhealthInterval * FACTOR_US_TO_MS;
}

} // namespace multi_call
