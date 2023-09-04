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
#ifndef ISEARCH_MULTI_CALL_RETRYLIMITCHECKER_H
#define ISEARCH_MULTI_CALL_RETRYLIMITCHECKER_H

#include <unordered_map>

#include "aios/network/gig/multi_call/common/common.h"
#include "autil/Lock.h"

namespace multi_call {

struct RetryCheckerItem {
    int32_t retryCountPerSecond = 0;
    int64_t retryTimeStamp = 0;
    autil::ReadWriteLock limitLock;
    bool canRetry(int64_t currentTimeInSeconds, int32_t retryCountLimit);

private:
    AUTIL_LOG_DECLARE();
};
class RetryLimitChecker
{
public:
    RetryLimitChecker();
    ~RetryLimitChecker();

private:
    RetryLimitChecker(const RetryLimitChecker &);
    RetryLimitChecker &operator=(const RetryLimitChecker &);

public:
    bool canRetry(const std::string &strategy, int64_t currentTimeInSeconds,
                  int32_t retryCountLimit);

private:
    std::unordered_map<std::string, std::unique_ptr<RetryCheckerItem>> _StrategyRetryChecker;
    autil::ReadWriteLock _checkerLock;
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(RetryLimitChecker);

} // namespace multi_call

#endif // ISEARCH_MULTI_CALL_RETRYLIMITCHECKER_H
