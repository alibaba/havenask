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
#ifndef ISEARCH_MULTI_CALL_QUERYFINOSTATISTICS_H
#define ISEARCH_MULTI_CALL_QUERYFINOSTATISTICS_H

#include "aios/network/gig/multi_call/agent/BizStat.h"
#include "aios/network/gig/multi_call/agent/WarmUpStrategy.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/metric/BizMetricReporter.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "autil/Log.h"

namespace multi_call {

class QueryInfoStatistics
{
public:
    QueryInfoStatistics(GigQueryInfo *queryInfo, WarmUpStrategy *warmUpStrategy,
                        BizStatPtr bizStat);
    ~QueryInfoStatistics();

public:
    float degradeLevel(float fullLevel);
    void fillResponseInfo(GigResponseInfo *responseInfo, float responseLatencyMs,
                          MultiCallErrorCode ec, WeightTy targetWeight);
    void reportBizStat(BizMetricReporter &bizReporter);

private:
    void initBizStat();
    void fillPropagationInfos(GigResponseInfo *responseInfo);
    void beginQuery(float percent);
    void endQuery();

private:
    float degradePercent() const;
    float degradePercentLatency() const;
    float degradePercentErrorRatio() const;
    void updateErrorRatio(MultiCallErrorCode ec, ServerRatioFilter &errorRatio);
    void updateDegradeRatio(MultiCallErrorCode ec, ServerRatioFilter &degradeRatio);
    void updateAverageLatency(MultiCallErrorCode ec, float responseLatencyMs,
                              AverageLatency &avgLatency);
    void updateLatency(MultiCallErrorCode ec, float responseLatencyMs);
    void updateLoadBalanceLatency(MultiCallErrorCode ec, float responseLatencyMs);
    void updateWarmUpStatus(GigResponseInfo *responseInfo);

private:
    GigQueryInfo *_queryInfo;
    BizStatPtr _bizStat;
    WarmUpStrategy *_warmUpStrategy;
    bool _queryCounted;
    float _degradePercent;
    float _delayMs;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(QueryInfoStatistics);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_QUERYFINOSTATISTICS_H
