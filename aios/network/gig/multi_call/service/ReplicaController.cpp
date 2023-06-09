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
#include "aios/network/gig/multi_call/service/ReplicaController.h"
#include "aios/network/gig/multi_call/metric/SnapshotInfoCollector.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, ReplicaController);

ReplicaController::ReplicaController(
    const SearchServiceProviderVector *serviceVector)
    : _serviceVector(serviceVector), _bestChain(nullptr), _multiVersion(false),
      _lastUpateTime(-1), _avgWeight(MAX_WEIGHT_FLOAT),
      _maxWeight(MAX_WEIGHT_FLOAT), _globalAvgLatency(0.0f),
      _bestLoadBalanceLatencyFilter(BEST_FILTER_INIT_LIMIT),
      _bestLoadBalanceDegradeRatioFilter(BEST_FILTER_INIT_LIMIT) {}

ReplicaController::~ReplicaController() {}

void ReplicaController::init(size_t bestChainQueueSize, bool multiVersion) {
    _bestChainQueue.resize(bestChainQueueSize, nullptr);
    _multiVersion = multiVersion;
    updateWeightStat();
}

ControllerChain *ReplicaController::getBestChain() const { return _bestChain; }

void ReplicaController::update(ControllerChain *candidate,
                               const MetricLimits &metricLimits) {
    updateBestChain(candidate, metricLimits);
    updateReplicaStat(autil::TimeUtility::currentTime());
}

void ReplicaController::updateBestChain(ControllerChain *candidate,
                                        const MetricLimits &metricLimits) {
    auto best = candidate;
    ControllerChain *toInsert = NULL;
    // may have dup element in multi-thread update, no need to fix
    if (_bestChainQueue.end() ==
        std::find(_bestChainQueue.begin(), _bestChainQueue.end(), candidate)) {
        toInsert = candidate;
    }
    for (size_t i = 0; i < _bestChainQueue.size(); i++) {
        auto current = _bestChainQueue[i];
        if (current == toInsert) {
            toInsert = NULL;
            continue;
        }
        if (current &&
            SearchServiceProvider::isBetterThan(current, best, metricLimits)) {
            best = current;
        }
        if (toInsert && SearchServiceProvider::isBetterThan(toInsert, current,
                                                            metricLimits)) {
            _bestChainQueue[i] = toInsert;
            toInsert = current;
        }
    }
    setBestChain(best);
}

void ReplicaController::setBestChain(ControllerChain *bestChain) {
    if (bestChain) {
        const auto &latencyController = bestChain->latencyController;
        const auto &degradeRatioController = bestChain->degradeRatioController;
        if (latencyController.isValid()) {
            float output = latencyController.controllerLoadBalanceOutput();
            if (INVALID_FILTER_VALUE != output) {
                _bestLoadBalanceLatencyFilter.update(output);
            }
        }
        if (degradeRatioController.isValid()) {
            float output = degradeRatioController.controllerLoadBalanceOutput();
            if (INVALID_FILTER_VALUE != output) {
                _bestLoadBalanceDegradeRatioFilter.update(output);
            }
        }
    }
    _bestChain = bestChain;
}

float ReplicaController::getAvgWeight() const { return _avgWeight; }

void ReplicaController::updateReplicaStat(int64_t currentTime) {
    {
        autil::ScopedReadWriteLock lock(_lock, 'r');
        if (_lastUpateTime != -1 &&
            currentTime - _lastUpateTime < REPLICA_STAT_UPDATE_INTERVAL) {
            return;
        }
    }
    if (0 == _lock.trywrlock()) {
        updateWeightStat();
        updateGlobalAvgLatency();
        _lastUpateTime = currentTime;
        _lock.unlock();
    }
}

void ReplicaController::updateWeightStat() {
    const SearchServiceProviderVector &serviceVector = *_serviceVector;
    WeightTy maxWeight = 0;
    WeightTy sum = 0;
    for (size_t i = 0; i < serviceVector.size(); i++) {
        // all weight >= 0
        auto weight = serviceVector[i]->getWeight();
        sum += weight;
        maxWeight = std::max(maxWeight, weight);
    }
    if (unlikely(0 == sum)) {
        if (!_multiVersion) {
            for (size_t i = 0; i < serviceVector.size(); i++) {
                serviceVector[i]->setWeight(MIN_INTERNAL_TARGET_WEIGHT_FLOAT);
            }
        }
        _avgWeight = MIN_INTERNAL_TARGET_WEIGHT_FLOAT;
        _maxWeight = MAX_WEIGHT_FLOAT;
    } else {
        _avgWeight = max((float)sum / serviceVector.size(),
                         MIN_INTERNAL_TARGET_WEIGHT_FLOAT);
        _maxWeight = maxWeight;
    }
}

void ReplicaController::updateGlobalAvgLatency() {
    const SearchServiceProviderVector &serviceVector = *_serviceVector;
    float weightedLatencySum = 0.0f;
    float serverWeightSum = 0.0f;
    float weightedNetLatency = 0.0f;
    float clientWeightSum = 0.0f;
    for (size_t i = 0; i < serviceVector.size(); i++) {
        auto &provider = *serviceVector[i];
        const auto &latencyController =
            provider.getControllerChain()->latencyController;
        float serverLatency = latencyController.loadBalanceServerValue();
        float serverWeight = latencyController.serverAvgWeight();
        if (serverWeight == INVALID_FILTER_VALUE ||
            serverLatency == INVALID_FILTER_VALUE) {
            continue;
        }
        weightedLatencySum += serverWeight * serverLatency;
        serverWeightSum += serverWeight;

        WeightTy clientWeight = provider.getWeight();
        float netLatency = latencyController.netLatency();
        weightedNetLatency += netLatency * clientWeight;
        clientWeightSum += clientWeight;
    }
    if (serverWeightSum > 0 && clientWeightSum > 0) {
        // add netLatency does not make sense, an approximation
        _globalAvgLatency = weightedLatencySum / serverWeightSum +
                            weightedNetLatency / clientWeightSum;
    } else {
        _globalAvgLatency = 0.0f;
    }
}

void ReplicaController::fillReplicaControllerInfo(
    ControllerFeedBack &feedBack) const {
    if (_bestLoadBalanceLatencyFilter.isValid()) {
        feedBack.bestLoadBalanceLatency =
            _bestLoadBalanceLatencyFilter.output();
    }
    if (_bestLoadBalanceDegradeRatioFilter.isValid()) {
        feedBack.bestLoadBalanceDegradeRatio =
            _bestLoadBalanceDegradeRatioFilter.output();
    }
    feedBack.bestChain = _bestChain;
    feedBack.maxWeight = _maxWeight;
}

bool ReplicaController::getBaseErrorRatio(float &baseErrorRatio) const {
    if (!_bestChain) {
        return false;
    }
    const auto &controller = _bestChain->errorRatioController;
    if (controller.isValid()) {
        baseErrorRatio = controller.controllerOutput();
        return baseErrorRatio != INVALID_FILTER_VALUE;
    } else {
        return false;
    }
}

bool ReplicaController::getBaseDegradeRatio(float &baseDegradeRatio) const {
    if (!_bestChain) {
        return false;
    }
    const auto &controller = _bestChain->degradeRatioController;
    if (controller.isValid()) {
        baseDegradeRatio = controller.controllerOutput();
        return baseDegradeRatio != INVALID_FILTER_VALUE;
    } else {
        return false;
    }
}

bool ReplicaController::getBaseAvgLatency(float &baseAvgLatency) const {
    if (_globalAvgLatency > 0.0f) {
        baseAvgLatency = _globalAvgLatency;
        return true;
    }
    if (!_bestChain) {
        return false;
    }
    const auto &controller = _bestChain->latencyController;
    if (controller.isValid()) {
        baseAvgLatency = controller.controllerOutput();
        return baseAvgLatency != INVALID_FILTER_VALUE;
    } else {
        return false;
    }
}

void ReplicaController::fillControllerInfo(SnapshotBizInfo &bizInfo) const {
    float baseAvgLatency = 0.0f;
    if (getBaseAvgLatency(baseAvgLatency)) {
        bizInfo.minProviderLatency =
            min(bizInfo.minProviderLatency, baseAvgLatency);
    }
    float baseErrorRatio = 0.0f;
    if (getBaseErrorRatio(baseErrorRatio)) {
        bizInfo.minErrorRatio = min(bizInfo.minErrorRatio, baseErrorRatio);
    }
    float baseDegradeRatio = 0.0f;
    if (getBaseDegradeRatio(baseDegradeRatio)) {
        bizInfo.minDegradeRatio =
            min(bizInfo.minDegradeRatio, baseDegradeRatio);
    }
    if (_bestLoadBalanceLatencyFilter.isValid()) {
        bizInfo.minLoadBalanceLatency =
            min(bizInfo.minLoadBalanceLatency,
                _bestLoadBalanceLatencyFilter.output());
    }
    if (_bestLoadBalanceDegradeRatioFilter.isValid()) {
        bizInfo.minLoadBalanceDegradeRatio =
            min(bizInfo.minLoadBalanceDegradeRatio,
                _bestLoadBalanceDegradeRatioFilter.output());
    }
    bizInfo.avgWeight = getAvgWeight();
}

void ReplicaController::toString(std::string &debugStr) const {
    debugStr += "   avgWeight: ";
    debugStr += StringUtil::fToString(_avgWeight);
    debugStr += ", maxWeight: ";
    debugStr += StringUtil::fToString(_maxWeight);
    debugStr += ", avgLat: ";
    debugStr += StringUtil::fToString(_globalAvgLatency);
    debugStr += ", lbLat: ";
    debugStr += StringUtil::fToString(_bestLoadBalanceLatencyFilter.output());
    debugStr += ", lbDegradeRatio: ";
    debugStr += StringUtil::fToString(_bestLoadBalanceDegradeRatioFilter.output());
    debugStr += ", providerCount: ";
    debugStr += StringUtil::toString(_serviceVector->size());
}

} // namespace multi_call
