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
#include "build_service/task_base/RestartIntervalController.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace build_service { namespace task_base {
BS_LOG_SETUP(task_base, RestartIntervalController);

RestartIntervalController::RestartIntervalController() : _retryInterval(0), _maxRetryIntervalTime(0) {}

RestartIntervalController::~RestartIntervalController() {}

void RestartIntervalController::wait()
{
    if (_retryInterval != 0) {
        BS_LOG(INFO, "restarting sleep interval in seconds[%ld]", _retryInterval / 1000000);
        usleep(_retryInterval);
        _retryInterval *= 2;
    } else {
        _retryInterval = 1000 * 1000;
    }

    if (_maxRetryIntervalTime == 0) {
        getMaxRetryIntervalTime();
    }
    if (_retryInterval > _maxRetryIntervalTime) {
        _retryInterval = _maxRetryIntervalTime;
    }
}

void RestartIntervalController::getMaxRetryIntervalTime()
{
    const char* param = getenv("max_retry_interval");
    int64_t maxRetryIntervalTime = 1800 * 1000 * 1000;
    _maxRetryIntervalTime = maxRetryIntervalTime;
    if (param && StringUtil::fromString(string(param), maxRetryIntervalTime)) {
        _maxRetryIntervalTime = maxRetryIntervalTime * 1000 * 1000;
    }
}

}} // namespace build_service::task_base
