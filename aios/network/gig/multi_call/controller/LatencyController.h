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
#ifndef ISEARCH_MULTI_CALL_LATENCYCONTROLLER_H
#define ISEARCH_MULTI_CALL_LATENCYCONTROLLER_H

#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
#include "aios/network/gig/multi_call/controller/ControllerBase.h"

namespace multi_call {

class LatencyController : public ControllerBase
{
public:
    LatencyController();

private:
    LatencyController(const LatencyController &);
    LatencyController &operator=(const LatencyController &);

public:
    float controllerOutput() const override;
    float netLatency() const;

public:
    float update(ControllerFeedBack &feedBack);
    bool isLegal(float providerAvgLatency, ControllerFeedBack &feedBack) const;
    bool isLegal(const ControllerChain *bestChain, const MetricLimits &metricLimits,
                 float responseLatency = INVALID_FILTER_VALUE) const;
    float getLegalLimit(const FlowControlConfigPtr &flowControlConfig) const;
    float getLoadBalanceLegalLimit(const FlowControlConfigPtr &flowControlConfig) const;

private:
    void updateLocalLatency(MultiCallErrorCode ec, float responseLatency);
    void updateServerLatency(QueryResultStatistic &stat, float latencyUpperLimitMs);
    void updateServerLatencyMirror(QueryResultStatistic &stat, float latencyUpperLimitMs);
    void doUpdateServerValue(const AverageLatency &avgLatency, float latencyUpperLimitMs);
    bool getLoadBalanceLowLimit(const ControllerFeedBack &feedBack, float &lowLimit) const;
    bool bestControllerValid(const ControllerChain *bestChain) const;

public:
    // load balance
    float updateLoadBalance(ControllerFeedBack &feedBack);
    float controllerLoadBalanceOutput() const;
    void updateNetLatencyFromHeartbeat(int64_t netLatencyUs);

private:
    static float adjustResponseLatency(const ControllerFeedBack &feedBack);
    static float getMaxAllowLatency(float bestLatency, const MetricLimits &metricLimits);
    static float getLatencyLimit(float base, const FlowControlConfigPtr &flowControlConfig);
    static float calculateWeightPercent(float lowLimit, float thisLatency,
                                        const MetricLimits &metricLimits);

private:
    Filter _netLatencyFilter;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(LatencyController);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_LATENCYCONTROLLER_H
