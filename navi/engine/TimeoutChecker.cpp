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
#include "navi/engine/TimeoutChecker.h"
#include "autil/TimeUtility.h"

namespace navi {

TimeoutChecker::TimeoutChecker(int64_t timeoutMs) {
    _begin = autil::TimeUtility::currentTime();
    setTimeoutMs(timeoutMs);
}

TimeoutChecker::~TimeoutChecker() {
}

void TimeoutChecker::setTimeoutMs(int64_t timeoutMs) {
    assert(_begin >= 0);
    _timeoutMs = timeoutMs;
    int64_t timeoutMsLimit = (std::numeric_limits<int64_t>::max() - _begin) / FACTOR_MS_TO_US;
    timeoutMs = std::max(-1L, std::min(timeoutMsLimit, timeoutMs));
    _end = _begin + timeoutMs * FACTOR_MS_TO_US;
}

int64_t TimeoutChecker::beginTime() const {
    return _begin;
}

int64_t TimeoutChecker::timeoutMs() const {
    return _timeoutMs;
}

int64_t TimeoutChecker::remainTime() const {
    return (_end - autil::TimeUtility::currentTime()) / FACTOR_MS_TO_US;
}

bool TimeoutChecker::timeout() const {
    return autil::TimeUtility::currentTime() > _end;
}

}
