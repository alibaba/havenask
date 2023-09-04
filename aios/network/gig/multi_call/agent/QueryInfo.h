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
#ifndef ISEARCH_MULTI_CALL_QUERYINFO_H
#define ISEARCH_MULTI_CALL_QUERYINFO_H

#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "autil/Lock.h"

namespace multi_call {

class GigStatistic;
class MetricReporterManager;
MULTI_CALL_TYPEDEF_PTR(MetricReporterManager);
class QueryInfoStatistics;
MULTI_CALL_TYPEDEF_PTR(QueryInfoStatistics);

class QueryInfo
{
public:
    QueryInfo(const std::string &queryInfoStr);
    QueryInfo(const std::string &queryInfoStr, const std::string &warmUpStrategy,
              GigStatistic *statistic,
              const std::shared_ptr<MetricReporterManager> &metricReporterManager);
    QueryInfo();
    virtual ~QueryInfo();

public:
    /* this is for server supporting multiLevel degrade strategy,
     * return value is between 0.0f - fullLevel, where larger value means
     * larger client side degrade
     */
    virtual float degradeLevel(float fullLevel) const;
    // 0.0f to 1.0f
    RequestType requestType() const;
    // return this string to client side gig, must be called
    virtual std::string
    finish(float responseLatencyMs /* ms */, MultiCallErrorCode ec,
           WeightTy targetWeight /* pass weight if needed else pass MAX_WEIGHT */);

    const std::string &getQueryInfoStr() const;
    const GigQueryInfo *getQueryInfo() const;
    const GigResponseInfo *getResponseInfo() const;
    bool isFinished() const;
    std::string toString() const;
    void setDecWeightFlag();

private:
    void init(const std::string &queryInfoStr);
    void fillGigMetaEnv();
    void reportQueryReceiveMetric();
    void reportAgentMetrics(float responseLatencyMs, MultiCallErrorCode ec, WeightTy targetWeight);
    bool getDecWeightFlag() const;

private:
    bool _finished;
    bool _validQueryInfo;
    google::protobuf::Arena _arena;
    std::string _queryInfoStr;
    GigQueryInfo *_queryInfo;
    GigResponseInfo *_responseInfo;
    QueryInfoStatisticsPtr _statistics;
    MetricReporterManagerPtr _metricReporterManager;
    mutable autil::ReadWriteLock _decWeightFlagLock;
    bool _decWeightFlag = false;
};

MULTI_CALL_TYPEDEF_PTR(QueryInfo);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_QUERYINFO_H
