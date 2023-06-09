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
#ifndef ISEARCH_MULTI_CALL_QUERYINFOIMPL_H
#define ISEARCH_MULTI_CALL_QUERYINFOIMPL_H

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/proto/GigAgent.pb.h"
#include "autil/StringUtil.h"

namespace multi_call {

class GigStatistic;
class BizStat;
class WarmUpStrategy;

struct QueryInfoImpl {
    QueryInfoImpl(const std::string &queryInfoStr_,
                  const std::string &warmUpStrategyName_,
                  GigStatistic *statistic);
    ~QueryInfoImpl();
    std::string toString() const;
    void beginQuery(float percent);
    void endQuery();
    void fillPropagationInfos();

private:
    void initBizStat(GigStatistic *statistic);

public:
    google::protobuf::Arena arena;
    bool finished;
    bool validQueryInfo;
    std::string queryInfoStr;
    std::string warmUpStrategyName;
    std::shared_ptr<BizStat> bizStat;
    WarmUpStrategy *warmUpStrategy;
    GigQueryInfo *queryInfo;
    GigResponseInfo *responseInfo;
    bool queryCounted;
    float degradePercent;
    float delayMs;
};

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_QUERYINFOIMPL_H
