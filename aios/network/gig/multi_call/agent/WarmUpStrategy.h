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
#ifndef ISEARCH_MULTI_CALL_WARMUPSTRATEGY_H
#define ISEARCH_MULTI_CALL_WARMUPSTRATEGY_H

#include "aios/network/anet/atomic.h"
#include "aios/network/gig/multi_call/common/ControllerParam.h"
#include "aios/network/gig/multi_call/common/common.h"

namespace multi_call {

class WarmUpStrategy
{
public:
    WarmUpStrategy(int64_t timeoutInSecond, int64_t queryCountLimit)
        : _timeout(0)
        , _queryCountLimit(0) {
        update(timeoutInSecond, queryCountLimit);
    }
    ~WarmUpStrategy() {
    }

private:
    WarmUpStrategy(const WarmUpStrategy &);
    WarmUpStrategy &operator=(const WarmUpStrategy &);

public:
    void update(int64_t timeoutInSecond, int64_t queryCountLimit) {
        _timeout = timeoutInSecond * FACTOR_S_TO_US;
        _queryCountLimit = queryCountLimit;
    }

public:
    int64_t _timeout;
    int64_t _queryCountLimit;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(WarmUpStrategy);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_WARMUPSTRATEGY_H
