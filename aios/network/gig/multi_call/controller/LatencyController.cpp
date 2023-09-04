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
#include "aios/network/gig/multi_call/controller/LatencyController.h"

#include "aios/network/gig/multi_call/controller/ControllerChain.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {
AUTIL_LOG_SETUP(multi_call, LatencyController);

LatencyController::LatencyController()
    : ControllerBase("latency")
    , _netLatencyFilter(NET_LATENCY_FILTER_INIT_LIMIT) {
}

float LatencyController::update(ControllerFeedBack &feedBack) {
    assert(!feedBack.mirrorResponse);
    updateServerLatency(feedBack.stat, feedBack.metricLimits.getLatencyUpperLimitMs());
    auto responseLatency = adjustResponseLatency(feedBack);
    updateLocalLatency(feedBack.stat.ec, responseLatency);
    float decStep = LATENCY_WEIGHT_DEC_STEP;
    if (hasServerAgent()) {
        if (!serverFilterReady()) {
            // have server agent and server not ready, need probe
            return decStep;
        }
    }
    if (isLegal(feedBack.bestChain, feedBack.metricLimits, responseLatency)) {
        return 0.0f;
    } else {
        return decStep;
    }
}

bool LatencyController::isLegal(const ControllerChain *bestChain, const MetricLimits &metricLimits,
                                float responseLatency) const {
    if (!bestControllerValid(bestChain)) {
        return true;
    }
    float providerAvgLatency = INVALID_FILTER_VALUE;
    if (isValid()) {
        providerAvgLatency = controllerOutput();
    } else if (INVALID_FILTER_VALUE == responseLatency) {
        // best chain select
        return false;
    } else {
        // use response latency as avg latency
        providerAvgLatency = responseLatency;
    }
    float bestLatency = min(providerAvgLatency, bestChain->latencyController.controllerOutput());
    float maxAllowedLatency = getMaxAllowLatency(bestLatency, metricLimits);
    return providerAvgLatency < maxAllowedLatency;
}

bool LatencyController::bestControllerValid(const ControllerChain *bestChain) const {
    if (!bestChain) {
        return false;
    }
    const auto &bestController = bestChain->latencyController;
    if (&bestController == this || !bestController.isValid()) {
        return false;
    }
    return true;
}

float LatencyController::adjustResponseLatency(const ControllerFeedBack &feedBack) {
    const auto &metricLimits = feedBack.metricLimits;
    return min(metricLimits.fullDegradeLatency * metricLimits.latencyUpperLimitPercent,
               (float)feedBack.stat.getRpcLatency() / FACTOR_US_TO_MS);
}

float LatencyController::controllerOutput() const {
    if (serverValueReady()) {
        auto ret = serverValue() + _netLatencyFilter.output();
        if (ret < INVALID_FILTER_VALUE) {
            return ret;
        }
    }
    return localValue();
}

void LatencyController::updateServerLatency(QueryResultStatistic &stat, float latencyUpperLimitMs) {
    if (!stat.agentInfo) {
        clearServerValue();
        return;
    }
    setHasServerAgent(true);
    const auto &agentInfo = *stat.agentInfo;
    auto end2EndLatency = stat.getRpcLatency() / FACTOR_US_TO_MS;
    if (agentInfo.latency_ms() >= end2EndLatency) {
        clearServerValue();
        return;
    }
    auto netLatency = end2EndLatency - agentInfo.latency_ms();
    stat.setNetLatency(netLatency);
    if (unlikely(0 == _netLatencyFilter.count())) {
        // ignore first net latency
        netLatency = 0.0f;
    }
    _netLatencyFilter.update(netLatency);
    const auto &avgLatency = agentInfo.avg_latency();
    if (invalidServerVersion(avgLatency)) {
        return;
    }
    setServerFilterReady(avgLatency.filter_ready());
    if (serverFilterReady()) {
        doUpdateServerValue(avgLatency, latencyUpperLimitMs);
    } else {
        clearServerValue();
    }
}

void LatencyController::updateServerLatencyMirror(QueryResultStatistic &stat,
                                                  float latencyUpperLimitMs) {
    if (!stat.validateMirror()) {
        return;
    }
    const auto &propagationStat = stat.getMirrorStat();
    const auto &latencyStat = propagationStat.latency();
    if (invalidServerVersion(latencyStat)) {
        return;
    }
    setServerFilterReady(latencyStat.filter_ready());
    if (serverFilterReady()) {
        doUpdateServerValue(latencyStat, latencyUpperLimitMs);
    } else {
        clearServerValue();
    }
}

void LatencyController::doUpdateServerValue(const AverageLatency &avgLatency,
                                            float latencyUpperLimitMs) {
    float latency = min(avgLatency.latency(), latencyUpperLimitMs);
    setServerValue(latency);
    if (avgLatency.has_load_balance_latency()) {
        float loadBalanceLatencyMs = min(avgLatency.load_balance_latency(), latencyUpperLimitMs);
        setLoadBalanceServerValue(loadBalanceLatencyMs);
    } else {
        // gig-2.1 compatible logic
        setLoadBalanceServerValue(latency);
    }
    setServerValueVersion(avgLatency.version());
    if (avgLatency.has_avg_weight()) {
        setServerAvgWeight(avgLatency.avg_weight());
    } else {
        setServerAvgWeight(INVALID_FILTER_VALUE);
    }
}

void LatencyController::updateLocalLatency(MultiCallErrorCode ec, float responseLatency) {
    if (ec <= MULTI_CALL_ERROR_DEC_WEIGHT || responseLatency > localValue()) {
        updateLocal(responseLatency);
    }
}

float LatencyController::getMaxAllowLatency(float bestLatency, const MetricLimits &metricLimits) {
    return min(bestLatency + metricLimits.latencyUpperLimitMs,
               bestLatency * metricLimits.latencyUpperLimitPercent);
}

float LatencyController::getLegalLimit(const FlowControlConfigPtr &flowControlConfig) const {
    if (!isValid()) {
        return INVALID_FILTER_VALUE;
    }
    auto output = controllerOutput();
    return getLatencyLimit(output, flowControlConfig);
}

float LatencyController::getLoadBalanceLegalLimit(
    const FlowControlConfigPtr &flowControlConfig) const {
    if (!isValid()) {
        return INVALID_FILTER_VALUE;
    }
    auto output = controllerLoadBalanceOutput();
    return getLatencyLimit(output, flowControlConfig);
}

float LatencyController::getLatencyLimit(float base,
                                         const FlowControlConfigPtr &flowControlConfig) {
    return min(base + flowControlConfig->latencyUpperLimitMs,
               base * (1.0f + flowControlConfig->latencyUpperLimitPercent));
}

float LatencyController::netLatency() const {
    return _netLatencyFilter.output();
}

float LatencyController::controllerLoadBalanceOutput() const {
    if (serverValueReady()) {
        auto ret = loadBalanceServerValue() + _netLatencyFilter.output();
        if (ret < INVALID_FILTER_VALUE) {
            return ret;
        }
    }
    return INVALID_FILTER_VALUE;
}

float LatencyController::updateLoadBalance(ControllerFeedBack &feedBack) {
    if (!hasServerAgent()) {
        return MAX_PERCENT;
    }
    auto &stat = feedBack.stat;
    if (feedBack.mirrorResponse) {
        updateServerLatencyMirror(stat, feedBack.metricLimits.getLatencyUpperLimitMs());
    }
    if (!serverFilterReady()) {
        return MAX_PERCENT;
    }
    if (!feedBack.mirrorResponse && stat.isEmptyAgentInfo()) {
        // empty agent info means query timeout
        return LOAD_BALANCE_MIN_PERCENT;
    }
    float lowLimit = INVALID_FILTER_VALUE;
    if (!getLoadBalanceLowLimit(feedBack, lowLimit)) {
        return MAX_PERCENT;
    }
    const auto &metricLimits = feedBack.metricLimits;
    auto returnPercent =
        calculateWeightPercent(lowLimit, controllerLoadBalanceOutput(), metricLimits);
    return std::max(LOAD_BALANCE_MIN_PERCENT, returnPercent);
}

bool LatencyController::getLoadBalanceLowLimit(const ControllerFeedBack &feedBack,
                                               float &lowLimit) const {
    auto bestChain = feedBack.bestChain;
    if (!bestControllerValid(bestChain)) {
        return false;
    }
    if (INVALID_FILTER_VALUE != feedBack.bestLoadBalanceLatency) {
        lowLimit = feedBack.bestLoadBalanceLatency;
    } else {
        lowLimit = bestChain->latencyController.controllerLoadBalanceOutput();
    }
    return true;
}

float LatencyController::calculateWeightPercent(float lowLimit, float thisLatency,
                                                const MetricLimits &metricLimits) {
    if (INVALID_FILTER_VALUE == lowLimit || INVALID_FILTER_VALUE == thisLatency) {
        return MAX_PERCENT;
    }
    auto upperLimit = getMaxAllowLatency(lowLimit, metricLimits);
    if (thisLatency <= lowLimit) {
        return MAX_PERCENT;
    } else if (thisLatency > upperLimit) {
        return MIN_PERCENT;
    } else {
        float range = upperLimit - lowLimit;
        if (range < EPSILON) {
            return MIN_PERCENT;
        } else {
            float rawValue = MAX_PERCENT - (thisLatency - lowLimit) / range;
            float negtiveRelu = std::min(MAX_PERCENT, rawValue * 2.0f);
            return negtiveRelu;
        }
    }
}

void LatencyController::updateNetLatencyFromHeartbeat(int64_t netLatencyUs) {
    if (0 == netLatencyUs) {
        return;
    }
    auto floatNetLatencyMs = netLatencyUs / FACTOR_US_TO_MS;
    if (unlikely(0 == _netLatencyFilter.count())) {
        _netLatencyFilter.setCurrent(floatNetLatencyMs);
        _netLatencyFilter.setCounter(1);
    } else {
        _netLatencyFilter.update(floatNetLatencyMs);
    }
}

} // namespace multi_call
