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
#pragma once

#include <cassert>
#include <cmath>
#include <functional>

#include "autil/Log.h"

namespace indexlib::util {

class RetryUtil
{
public:
    enum class BackOffStrategy { FIXED, EXPONENTIAL };

    struct RetryOptions {
        RetryOptions() : maxRetryTime(3), retryIntervalInSecond(1), strategy(BackOffStrategy::FIXED) {}
        int maxRetryTime;
        int retryIntervalInSecond;
        BackOffStrategy strategy;
    };
    static bool Retry(std::function<bool()> func, const RetryOptions& retryOptions = RetryOptions(),
                      std::function<void()> failCallBack = nullptr);
    // retryE throw exception when retry time exceed
    static void RetryE(std::function<void()> func, const RetryOptions& retryOptions = RetryOptions(),
                       std::function<void()> exceptionCallBack = nullptr);
    // retryCount start from 0
    static int GetRetryInterval(const RetryOptions& retryOptions, int retryCount);

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::util
