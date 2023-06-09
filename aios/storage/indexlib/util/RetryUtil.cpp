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
#include "indexlib/util/RetryUtil.h"

#include <functional>
#include <thread>
#include <unistd.h>

namespace indexlib::util {
AUTIL_LOG_SETUP(indexlib.util, RetryUtil);

bool RetryUtil::Retry(std::function<bool()> func, const RetryOptions& retryOptions, std::function<void()> failCallBack)
{
    assert(retryOptions.maxRetryTime >= 0);
    for (int count = 0; count < retryOptions.maxRetryTime + 1; count++) {
        if (func()) {
            return true;
        }
        if (failCallBack) {
            failCallBack();
        }
        if (count >= retryOptions.maxRetryTime) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::seconds(GetRetryInterval(retryOptions, count)));
    }
    return false;
}

void RetryUtil::RetryE(std::function<void()> func, const RetryOptions& retryOptions,
                       std::function<void()> exceptionCallBack)
{
    assert(retryOptions.maxRetryTime >= 0);
    for (int count = 0; count < retryOptions.maxRetryTime + 1; count++) {
        try {
            return func();
        } catch (...) {
            if (exceptionCallBack) {
                exceptionCallBack();
            }
            if (count >= retryOptions.maxRetryTime) {
                throw;
            }
            std::this_thread::sleep_for(std::chrono::seconds(GetRetryInterval(retryOptions, count)));
        }
    }
}

int RetryUtil::GetRetryInterval(const RetryOptions& retryOptions, int retryCount)
{
    assert(retryCount >= 0);
    if (retryOptions.strategy == BackOffStrategy::FIXED) {
        return retryOptions.retryIntervalInSecond;
    } else if (retryOptions.strategy == BackOffStrategy::EXPONENTIAL) {
        return pow(2, retryCount) * retryOptions.retryIntervalInSecond;
    }
    return 0;
}

} // namespace indexlib::util
