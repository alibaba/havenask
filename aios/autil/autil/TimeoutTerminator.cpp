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
#include "autil/TimeoutTerminator.h"

#include <cassert>
#include <limits>

namespace autil {
TimeoutTerminator::TimeoutTerminator() : TimeoutTerminator(std::numeric_limits<int64_t>::max(), autil::TimeUtility::currentTime()) {}
TimeoutTerminator::TimeoutTerminator(int64_t timeout) : TimeoutTerminator(timeout, autil::TimeUtility::currentTime()) {}

TimeoutTerminator::TimeoutTerminator(int64_t timeout, int64_t startTime) {
    _expireTime = TIME_ADD(startTime, timeout);
    _timeout = _expireTime - startTime;
    _startTime = startTime;
    init(1, DEFAULT_RESTRICTOR_CHECK_MASK + 1);
}

TimeoutTerminator::~TimeoutTerminator() {}

void TimeoutTerminator::init(int32_t checkStep) { init(checkStep, 16); }

void TimeoutTerminator::init(int32_t checkStep, int32_t restrictStep) {
    assert(checkStep > 0);
    assert(((checkStep - 1) & checkStep) == 0);
    _checkMask = checkStep - 1;
    _checkTimes = 0;

    assert(restrictStep > 0);
    assert(((restrictStep - 1) & restrictStep) == 0);
    _restrictorCheckMask = restrictStep - 1;
    _restrictorCheckTimes = 0;

    _isTimeout = false;
    _isTerminated = false;
}

int32_t TimeoutTerminator::getCheckTimes() const { return _checkTimes; }
int32_t TimeoutTerminator::getRestrictorCheckTimes() const { return _restrictorCheckTimes; }
int64_t TimeoutTerminator::getLeftTime() {
    return _expireTime - getCurrentTime();
}
int64_t TimeoutTerminator::getExpireTime() const { return _expireTime; }
int64_t TimeoutTerminator::getStartTime() const { return _startTime; }
int64_t TimeoutTerminator::getTimeout() const { return _timeout; }
bool TimeoutTerminator::hasTimeout() const { return _expireTime < std::numeric_limits<int64_t>::max(); }
void TimeoutTerminator::updateExpireTime(int64_t expireTime) { _expireTime = expireTime; }

} // namespace autil
