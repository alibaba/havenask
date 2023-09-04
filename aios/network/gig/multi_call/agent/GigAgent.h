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
#ifndef ISEARCH_MULTI_CALL_GIGAGENT_H
#define ISEARCH_MULTI_CALL_GIGAGENT_H

#include "aios/network/gig/multi_call/agent/QueryInfo.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/MultiCallConfig.h"

namespace multi_call {

class BizStat;
class GigStatistic;

MULTI_CALL_TYPEDEF_PTR(QueryInfo);

class GigAgent
{
public:
    GigAgent();
    ~GigAgent();

private:
    GigAgent(const GigAgent &);
    GigAgent &operator=(const GigAgent &);

public:
    QueryInfoPtr getQueryInfo(const std::string &queryInfo, const std::string &warmUpStrategy = "",
                              bool withBizStat = true);
    void updateWarmUpStrategy(const std::string &warmUpStrategy, int64_t timeoutInSecond,
                              int64_t queryCountLimit);
    bool init(const std::string &logPrefix = "", bool enableAgentStat = true);
    void start();
    void stop();
    bool isStopped() const;
    void start(const std::string &bizName, PartIdTy partId = INVALID_PART_ID);
    void stop(const std::string &bizName, PartIdTy partId = INVALID_PART_ID);
    bool isStopped(const std::string &bizName, PartIdTy partId = INVALID_PART_ID) const;

    bool longTimeNoQuery(int64_t noQueryTimeInSecond) const;
    bool longTimeNoQuery(const std::string &bizName, int64_t noQueryTimeInSecond) const;
    std::string getLogPrefix() const;

private:
    std::shared_ptr<BizStat> getBizStat(const std::string &bizName, PartIdTy partId);
    friend class HeartbeatServerManager;

private:
    GigStatistic *_statistic;
    std::shared_ptr<MetricReporterManager> _metricReporterManager;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(GigAgent);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_GIGAGENT_H
