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
#include "aios/network/gig/multi_call/agent/BizStat.h"

#include <math.h>

#include "aios/network/gig/multi_call/agent/WarmUpStrategy.h"
#include "autil/StringUtil.h"

using namespace std;

namespace multi_call {
AUTIL_DECLARE_AND_SETUP_LOGGER(multi_call, BizStat);

bool PropagationStat::update(const PropagationStatDef &stat, int64_t &delay) {
    const auto &errorFilter = stat.error();
    const auto &degradeFilter = stat.degrade();
    const auto &avgLatency = stat.latency();
    auto newVersion = avgLatency.version();
    auto currentVersion = version;
    if (newVersion <= currentVersion) {
        return false;
    }
    errorMetric.setMetric(errorFilter.ratio(), errorFilter.load_balance_ratio());
    degradeMetric.setMetric(degradeFilter.ratio(), degradeFilter.load_balance_ratio());
    latencyMetric.setMetric(avgLatency.latency(), avgLatency.load_balance_latency());
    agentId = avgLatency.agent_id();
    avgWeight = avgLatency.avg_weight();
    if (currentVersion > 0) {
        delay = newVersion - currentVersion;
    }
    version = newVersion;
    return true;
}

void BizStat::beginQuery(bool degrade) {
    InProcessingQueryCount currentCount;
    InProcessingQueryCount newCount;
    do {
        currentCount = processingCount;
        newCount.degradeCount = currentCount.degradeCount + (degrade == true);
        newCount.normalCount = currentCount.normalCount + (degrade == false);
    } while (!cmpxchg(&processingCount.value, currentCount.value, newCount.value));
}

void BizStat::endQuery(bool degrade) {
    InProcessingQueryCount currentCount;
    InProcessingQueryCount newCount;
    do {
        currentCount = processingCount;
        newCount.degradeCount = max(0, currentCount.degradeCount - (degrade == true));
        newCount.normalCount = max(0, currentCount.normalCount - (degrade == false));
    } while (!cmpxchg(&processingCount.value, currentCount.value, newCount.value));
}

float BizStat::updatePropagationStats(const PropagationStats &propagationStats) {
    int64_t delaySum = 0;
    int64_t delayCount = 0;
    for (int32_t i = 0; i < propagationStats.stats_size(); i++) {
        const auto &newStat = propagationStats.stats(i);
        const auto &avgLatency = newStat.latency();
        auto propagationStat = getPropagationStat(avgLatency.agent_id());
        int64_t delay = 0;
        if (propagationStat->update(newStat, delay)) {
            delaySum += delay;
            delayCount++;
        }
    }
    if (delayCount > 0) {
        return (float)delaySum / delayCount / FACTOR_US_TO_MS;
    } else {
        return INVALID_FILTER_VALUE;
    }
}

void BizStat::fillPropagationInfos(PropagationStats &propagationStats) {
    auto beginIndex = randomGenerator.get();
    PropagationStat *beginStat = NULL;
    for (size_t i = 0; i < PAYLOAD_SIZE; i++) {
        auto propagationStat = getPropagationStatByIndex(beginIndex + i);
        if (!propagationStat) {
            return;
        }
        if (!beginStat) {
            beginStat = propagationStat;
        } else if (beginStat == propagationStat) {
            break;
        }
        auto pbInfo = propagationStats.add_stats();

        auto version = propagationStat->version;
        auto errorRatio = pbInfo->mutable_error();
        errorRatio->set_filter_ready(true);
        errorRatio->set_ratio(propagationStat->errorMetric.value);
        errorRatio->set_load_balance_ratio(propagationStat->errorMetric.loadBalanceValue);

        auto degradeRatio = pbInfo->mutable_degrade();
        degradeRatio->set_filter_ready(true);
        degradeRatio->set_ratio(propagationStat->degradeMetric.value);
        degradeRatio->set_load_balance_ratio(propagationStat->degradeMetric.loadBalanceValue);

        auto latency = pbInfo->mutable_latency();
        latency->set_filter_ready(true);
        latency->set_agent_id(propagationStat->agentId);
        latency->set_version(version);
        latency->set_latency(propagationStat->latencyMetric.value);
        latency->set_load_balance_latency(propagationStat->latencyMetric.loadBalanceValue);
        latency->set_avg_weight(propagationStat->avgWeight);
    }
}

PropagationStat *BizStat::getPropagationStat(uint64_t agentId) {
    {
        autil::ScopedReadLock lock(_statLock);
        auto it = _statMap.find(agentId);
        if (_statMap.end() != it) {
            return it->second;
        }
    }
    {
        autil::ScopedWriteLock lock(_statLock);
        auto it = _statMap.find(agentId);
        if (_statMap.end() != it) {
            return it->second;
        }
        auto stat = new PropagationStat(agentId);
        _statMap.insert(make_pair(agentId, stat));
        _statArray.push_back(stat);
        return stat;
    }
}

PropagationStat *BizStat::getPropagationStatByIndex(uint64_t index) {
    autil::ScopedReadLock lock(_statLock);
    if (_statArray.empty()) {
        return NULL;
    }
    index = index % _statArray.size();
    return _statArray[index];
}

void BizStat::fillErrorRatio(ServerRatioFilter &errorRatio) const {
    errorRatio.set_filter_ready(errorMetricFilter.value.isValid());
    errorRatio.set_ratio(min((float)MAX_RATIO, errorMetricFilter.value.output()));
    errorRatio.set_load_balance_ratio(
        min((float)MAX_RATIO, errorMetricFilter.loadBalanceValue.output()));
}

void BizStat::fillDegradeRatio(ServerRatioFilter &degradeRatio) const {
    constexpr float maxRatio = MAX_RATIO;
    degradeRatio.set_filter_ready(degradeMetricFilter.value.isValid());
    degradeRatio.set_ratio(min(maxRatio, degradeMetricFilter.value.output()));
    degradeRatio.set_load_balance_ratio(min(maxRatio, getAdjustLbDegradeRatio()));
}

void BizStat::fillAvgLatency(AverageLatency &avgLatency) const {
    const auto &latencyFilter = latencyMetricFilter.value;
    avgLatency.set_filter_ready(latencyFilter.isValid());
    avgLatency.set_version(autil::TimeUtility::currentTime());
    avgLatency.set_latency(latencyFilter.output());
    avgLatency.set_load_balance_latency(getAdjustedLoadBalanceLatency());
    avgLatency.set_avg_weight(weightFilter.output());
}

float BizStat::getAdjustLbDegradeRatio() const {
    const auto &degradeFilter = degradeMetricFilter.loadBalanceValue;
    auto degradeRatio = degradeFilter.output();
    auto windowSize = degradeFilter.getWindowSize();
    return (windowSize * degradeRatio + processingCount.degradeCount * MAX_RATIO) /
           (windowSize + processingCount.degradeCount + processingCount.normalCount);
}

float BizStat::getAdjustedLoadBalanceLatency() const {
    const auto &latencyFilter = latencyMetricFilter.loadBalanceValue;
    auto avgLatency = latencyFilter.output();
    auto queryCount = processingCount;
    if (0 == queryCount.value) {
        return avgLatency;
    }
    auto alpha = latencyFilter.getAlpha();
    avgLatency = doAdjustLatency(alpha, avgLatency, normalLatencyFilter, queryCount.normalCount);
    avgLatency = doAdjustLatency(alpha, avgLatency, degradeLatencyFilter, queryCount.degradeCount);
    return avgLatency;
}

float BizStat::doAdjustLatency(float alpha, float metric, const Filter &filter, int32_t count) {
    if (count > 0 && filter.isValid()) {
        auto compensate = filter.output();
        return powf(1.0f - alpha, count) * (metric - compensate) + compensate;
    } else {
        return metric;
    }
}

void BizStat::fillThisPropagationStat(PropagationStatDef *statDef) const {
    fillErrorRatio(*statDef->mutable_error());
    fillDegradeRatio(*statDef->mutable_degrade());
    fillAvgLatency(*statDef->mutable_latency());
}

BizStat::~BizStat() {
    clear();
}

void BizStat::clear() {
    autil::ScopedWriteLock lock(_statLock);
    for (const auto &pair : _statMap) {
        delete pair.second;
    }
    _statArray.clear();
    _statMap.clear();
}

int64_t BizStat::count() const {
    return errorMetricFilter.value.count();
}

bool BizStat::warmUpFinished(WarmUpStrategy *strategy) {
    auto currentTime = autil::TimeUtility::currentTime();
    if (beginTimeStamp == -1) {
        beginTimeStamp = currentTime;
    }
    if (!strategy) {
        return true;
    }
    return count() >= strategy->_queryCountLimit ||
           beginTimeStamp + strategy->_timeout <= currentTime;
}

void BizStat::resetFilters() {
    errorMetricFilter.reset();
    degradeMetricFilter.reset();
    latencyMetricFilter.reset();
    normalLatencyFilter.reset();
    degradeLatencyFilter.reset();
}

std::string BizStat::toString() const {
    string str;
    str += "started: " + autil::StringUtil::toString(started);
    str += ", beginTime: " + autil::StringUtil::toString(beginTimeStamp);
    str += ", lastQueryTime: " + autil::StringUtil::toString(lastQueryTimestamp);
    str += "\n      ErrorRatio:   " + errorMetricFilter.value.toString();
    str += "\n      LBErrorRatio:   " + errorMetricFilter.loadBalanceValue.toString();
    str += "\n      DegradeRatio: " + degradeMetricFilter.value.toString();
    str += "\n      LBDegradeRatio: " + degradeMetricFilter.loadBalanceValue.toString();
    str += "\n      Latency:      " + latencyMetricFilter.value.toString();
    str += "\n      LBLatency:      " + latencyMetricFilter.loadBalanceValue.toString();
    str += "\n      processingCount: " + autil::StringUtil::toString(processingCount.degradeCount) +
           " D " + autil::StringUtil::toString(processingCount.normalCount) + " N";
    str += ", latNorm: " + autil::StringUtil::toString(normalLatencyFilter.output()) +
           ", latD: " + autil::StringUtil::toString(degradeLatencyFilter.output());
    str += "\n      weightAvg: " + autil::StringUtil::toString(weightFilter.output());
    str += "\n";
    return str;
}

} // namespace multi_call
