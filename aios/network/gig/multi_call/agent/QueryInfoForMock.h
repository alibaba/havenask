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
#ifndef ISEARCH_MULTI_CALL_QUERYINFOFORMOCK_H
#define ISEARCH_MULTI_CALL_QUERYINFOFORMOCK_H

#include "aios/network/gig/multi_call/agent/QueryInfo.h"

namespace multi_call {

class GigStatistic;

class QueryInfoForMock : public QueryInfo
{
public:
    QueryInfoForMock();
    ~QueryInfoForMock();

private:
    QueryInfoForMock(const QueryInfoForMock &);
    QueryInfoForMock &operator=(const QueryInfoForMock &);

public:
    static std::string createResponseInfo(float latencyMs, multi_call::MultiCallErrorCode ec,
                                          float errorRatio, float degradeRatio, float avgLatency,
                                          WeightTy targetWeight, bool warmupFinished);

private:
    GigStatistic *_statistic;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(QueryInfoForMock);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_QUERYINFOFORMOCK_H
