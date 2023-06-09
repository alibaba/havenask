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
#ifndef ISEARCH_MULTI_CALL_CONTROLLERFEEDBACK_H
#define ISEARCH_MULTI_CALL_CONTROLLERFEEDBACK_H

#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/QueryResultStatistic.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

struct MetricLimits {
    MetricLimits()
        : errorRatioLimit(MAX_PERCENT),
          latencyUpperLimitMs(DEFAULT_LATENCY_UPPER_LIMIT_MS),
          latencyUpperLimitPercent(DEFAULT_LATENCY_UPPER_LIMIT_PERCENT),
          fullDegradeLatency(DEFAULT_DEGRADE_LATENCY) {}
    bool operator==(const MetricLimits &rhs) const {
        return errorRatioLimit == rhs.errorRatioLimit &&
               latencyUpperLimitMs == rhs.latencyUpperLimitMs &&
               latencyUpperLimitPercent == rhs.latencyUpperLimitPercent &&
               fullDegradeLatency == rhs.fullDegradeLatency;
    }
    float getLatencyUpperLimitMs() {
        return std::min(latencyUpperLimitMs + fullDegradeLatency,
                        fullDegradeLatency * latencyUpperLimitPercent);
    }
    float errorRatioLimit;
    float latencyUpperLimitMs;
    float latencyUpperLimitPercent; // this percent already add 1
    float fullDegradeLatency;
};

struct ControllerChain;

struct ControllerFeedBack {
    ControllerFeedBack(QueryResultStatistic &stat_)
        : mirrorResponse(false), stat(stat_), bestChain(NULL),
          maxWeight(MAX_WEIGHT_FLOAT), minWeight(MIN_WEIGHT_FLOAT),
          bestLoadBalanceLatency(INVALID_FILTER_VALUE),
          bestLoadBalanceDegradeRatio(INVALID_FILTER_VALUE) {}
    ControllerFeedBack &operator=(const ControllerFeedBack &) = delete;

private:
    // for ut
    bool operator==(const ControllerFeedBack &rhs) const {
        return mirrorResponse == rhs.mirrorResponse && &stat == &rhs.stat &&
               bestChain == rhs.bestChain && metricLimits == rhs.metricLimits &&
               maxWeight == rhs.maxWeight && minWeight == rhs.minWeight &&
               bestLoadBalanceLatency == rhs.bestLoadBalanceLatency &&
               bestLoadBalanceDegradeRatio == rhs.bestLoadBalanceDegradeRatio;
    }

public:
    bool mirrorResponse;
    QueryResultStatistic &stat;
    const ControllerChain *bestChain;
    MetricLimits metricLimits;
    float maxWeight;
    float minWeight;
    float bestLoadBalanceLatency;
    float bestLoadBalanceDegradeRatio;
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_CONTROLLERFEEDBACK_H
