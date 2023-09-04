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
#include "aios/network/gig/multi_call/agent/QueryInfoStatistics.h"

#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/util/MiscUtil.h"

using namespace std;
using namespace autil;

namespace multi_call {

AUTIL_LOG_SETUP(multi_call, QueryInfoStatistics);

QueryInfoStatistics::QueryInfoStatistics(GigQueryInfo *queryInfo, WarmUpStrategy *warmUpStrategy,
                                         BizStatPtr bizStat)
    : _queryInfo(queryInfo)
    , _bizStat(bizStat)
    , _warmUpStrategy(warmUpStrategy)
    , _queryCounted(false)
    , _degradePercent(MIN_PERCENT)
    , _delayMs(INVALID_FILTER_VALUE) {
    assert(_bizStat);
    initBizStat();
}

QueryInfoStatistics::~QueryInfoStatistics() {
}

void QueryInfoStatistics::initBizStat() {
    _delayMs = _bizStat->updatePropagationStats(_queryInfo->propagation_stats());
    _bizStat->weightFilter.update(_queryInfo->gig_weight());
    bool normalRequest = !_queryInfo->has_request_type() // query from anywhere but gig client
                         || RT_NORMAL == _queryInfo->request_type(); // query from gig client
    bool userProbe =
        _queryInfo->has_user_request_type() && RT_NORMAL != _queryInfo->user_request_type();
    if (normalRequest && !userProbe) {
        _bizStat->lastQueryTimestamp = autil::TimeUtility::currentTimeInMicroSeconds();
    }
}

float QueryInfoStatistics::degradeLevel(float fullLevel) {
    if (fullLevel <= MIN_PERCENT) {
        beginQuery(MIN_PERCENT);
        return MIN_PERCENT;
    }
    auto percent = degradePercent();
    auto rand = _bizStat->randomGenerator.get() % MAX_WEIGHT;
    const auto &degradeFilter = _bizStat->degradeMetricFilter.loadBalanceValue;
    if (percent != MIN_PERCENT && degradeFilter.isValid()) {
        auto adjustedDegradeRatio = _bizStat->getAdjustLbDegradeRatio();
        if (rand > percent * MAX_WEIGHT_FLOAT || adjustedDegradeRatio > percent * MAX_RATIO) {
            percent = MIN_PERCENT;
        }
    }
    beginQuery(percent);
    return percent * fullLevel;
}

float QueryInfoStatistics::degradePercent() const {
    return max(degradePercentLatency(), degradePercentErrorRatio());
}

float QueryInfoStatistics::degradePercentLatency() const {
    const auto &queryInfo = *_queryInfo;
    if (!queryInfo.has_begin_server_degrade_latency() || !queryInfo.has_begin_degrade_latency()) {
        return MIN_PERCENT;
    }
    const auto &latencyFilter = _bizStat->latencyMetricFilter.loadBalanceValue;
    if (!latencyFilter.isValid()) {
        return MIN_PERCENT;
    }
    auto adjustedAvgLatency = _bizStat->getAdjustedLoadBalanceLatency();
    return MiscUtil::scaleLinear(queryInfo.begin_server_degrade_latency(),
                                 queryInfo.begin_degrade_latency(), adjustedAvgLatency);
}

float QueryInfoStatistics::degradePercentErrorRatio() const {
    const auto &queryInfo = *_queryInfo;
    if (!queryInfo.has_begin_server_degrade_error_ratio() ||
        !queryInfo.has_begin_degrade_error_ratio()) {
        return MIN_PERCENT;
    }
    const auto &errorFilter = _bizStat->errorMetricFilter.loadBalanceValue;
    if (!errorFilter.isValid()) {
        return MIN_PERCENT;
    }
    auto errorRatio = errorFilter.output();
    return MiscUtil::scaleLinear(queryInfo.begin_server_degrade_error_ratio() * MAX_RATIO,
                                 queryInfo.begin_degrade_error_ratio() * MAX_RATIO, errorRatio);
}

void QueryInfoStatistics::beginQuery(float percent) {
    _degradePercent = percent;
    _bizStat->beginQuery(_degradePercent > MIN_PERCENT);
    _queryCounted = true;
}

void QueryInfoStatistics::fillResponseInfo(GigResponseInfo *responseInfo, float responseLatencyMs,
                                           MultiCallErrorCode ec, WeightTy targetWeight) {
    if (unlikely(!_bizStat->started)) {
        targetWeight = MIN_WEIGHT;
    } else {
        updateErrorRatio(ec, *responseInfo->mutable_error_ratio());
        updateDegradeRatio(ec, *responseInfo->mutable_degrade_ratio());
        updateAverageLatency(ec, responseLatencyMs, *responseInfo->mutable_avg_latency());
        updateWarmUpStatus(responseInfo);
    }
    responseInfo->set_target_weight(targetWeight);
    _bizStat->fillPropagationInfos(*responseInfo->mutable_propagation_stats());
    endQuery();
}

void QueryInfoStatistics::updateErrorRatio(MultiCallErrorCode ec, ServerRatioFilter &errorRatio) {
    float error = ec > MULTI_CALL_ERROR_DEC_WEIGHT ? MAX_RATIO : MIN_RATIO;
    auto errorRatioLimit = INVALID_FILTER_VALUE;
    const auto &queryInfo = *_queryInfo;
    if (queryInfo.has_error_ratio_limit()) {
        errorRatioLimit = queryInfo.error_ratio_limit();
    }
    auto &errorMetricFilter = _bizStat->errorMetricFilter;
    errorMetricFilter.update(error, errorRatioLimit, errorRatioLimit);

    _bizStat->fillErrorRatio(errorRatio);
}

void QueryInfoStatistics::updateDegradeRatio(MultiCallErrorCode ec,
                                             ServerRatioFilter &degradeRatio) {
    float degrade = MIN_RATIO;
    if (MULTI_CALL_ERROR_DEC_WEIGHT == ec) {
        if (_degradePercent == MIN_PERCENT) {
            // app decided degrade
            degrade = MAX_RATIO;
        } else {
            degrade = _degradePercent * MAX_RATIO;
        }
    }
    constexpr float maxRatio = MAX_RATIO;
    auto &degradeMetricFilter = _bizStat->degradeMetricFilter;
    degradeMetricFilter.update(degrade, maxRatio, maxRatio);

    _bizStat->fillDegradeRatio(degradeRatio);
}

void QueryInfoStatistics::updateAverageLatency(MultiCallErrorCode ec, float responseLatencyMs,
                                               AverageLatency &avgLatency) {
    updateLatency(ec, responseLatencyMs);
    updateLoadBalanceLatency(ec, responseLatencyMs);

    _bizStat->fillAvgLatency(avgLatency);
}

void QueryInfoStatistics::updateLatency(MultiCallErrorCode ec, float responseLatencyMs) {
    auto &latencyFilter = _bizStat->latencyMetricFilter.value;
    auto latencyLimit = INVALID_FILTER_VALUE;
    const auto &queryInfo = *_queryInfo;
    if (queryInfo.has_latency_limit_ms()) {
        latencyLimit = queryInfo.latency_limit_ms();
    }
    if (ec <= MULTI_CALL_ERROR_DEC_WEIGHT || responseLatencyMs > (uint64_t)latencyFilter.output()) {
        latencyFilter.update(responseLatencyMs, latencyLimit);
    }
}

void QueryInfoStatistics::updateLoadBalanceLatency(MultiCallErrorCode ec, float responseLatencyMs) {
    auto loadBalanceLatency = responseLatencyMs;
    const auto &queryInfo = *_queryInfo;
    if (queryInfo.has_load_balance_latency_limit_ms()) {
        loadBalanceLatency = min(loadBalanceLatency, queryInfo.load_balance_latency_limit_ms());
    }

    auto &loadBalanceLatencyFilter = _bizStat->latencyMetricFilter.loadBalanceValue;
    if (ec <= MULTI_CALL_ERROR_DEC_WEIGHT ||
        responseLatencyMs > (uint64_t)loadBalanceLatencyFilter.output()) {
        loadBalanceLatencyFilter.update(loadBalanceLatency);

        auto &normalLatencyFilter = _bizStat->normalLatencyFilter;
        auto &degradeLatencyFilter = _bizStat->degradeLatencyFilter;
        if (ec == MULTI_CALL_ERROR_DEC_WEIGHT) {
            degradeLatencyFilter.update(loadBalanceLatency);
        } else {
            normalLatencyFilter.update(loadBalanceLatency);
        }
    }
}

void QueryInfoStatistics::updateWarmUpStatus(GigResponseInfo *responseInfo) {
    auto &warmUpStatus = *responseInfo->mutable_warm_up_status();
    auto finished = _bizStat->warmUpFinished(_warmUpStrategy);
    warmUpStatus.set_finished(finished);
}

void QueryInfoStatistics::endQuery() {
    if (_queryCounted) {
        _bizStat->endQuery(_degradePercent > MIN_PERCENT);
        _queryCounted = false;
    }
}

void QueryInfoStatistics::reportBizStat(BizMetricReporter &bizReporter) {
    bizReporter.reportAgentErrorRatio(_bizStat->errorMetricFilter.value.output());
    bizReporter.reportAgentLBErrorRatio(_bizStat->errorMetricFilter.loadBalanceValue.output());

    bizReporter.reportAgentDegradeRatio(_bizStat->degradeMetricFilter.value.output());
    bizReporter.reportAgentLBDegradeRatio(_bizStat->degradeMetricFilter.loadBalanceValue.output());

    bizReporter.reportAgentAvgLatency(_bizStat->latencyMetricFilter.value.output());
    bizReporter.reportAgentLBAvgLatency(_bizStat->latencyMetricFilter.loadBalanceValue.output());

    bizReporter.reportAgentNormalProcessingCount(_bizStat->processingCount.normalCount);
    bizReporter.reportAgentDegradeProcessingCount(_bizStat->processingCount.degradeCount);
    bizReporter.reportAgentNormalAvgLatency(_bizStat->normalLatencyFilter.output());
    bizReporter.reportAgentDegradeAvgLatency(_bizStat->degradeLatencyFilter.output());

    bizReporter.reportAgentAvgWeight(_bizStat->weightFilter.output());
    if (_delayMs != INVALID_FILTER_VALUE) {
        bizReporter.reportAgentMetricAvgDelay(_delayMs);
    }
    bizReporter.reportAgentQueryCount(_bizStat->count());
}

} // namespace multi_call
