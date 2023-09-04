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
#ifndef NAVI_TIMEOUTCHECKER_H
#define NAVI_TIMEOUTCHECKER_H

#include "navi/common.h"

namespace navi {

class TimeoutChecker
{
public:
    TimeoutChecker(int64_t timeoutMs = DEFAULT_TIMEOUT_MS);
    ~TimeoutChecker();
private:
    TimeoutChecker(const TimeoutChecker &);
    TimeoutChecker &operator=(const TimeoutChecker &);
public:
    void setTimeoutMs(int64_t timeoutMs);
    int64_t beginTime() const;
    int64_t timeoutMs() const;
    bool timeout() const;
    int64_t remainTime() const;
    int64_t elapsedTime() const;
private:
    int64_t _timeoutMs;
    int64_t _begin;
    int64_t _end;
};

}

#endif //NAVI_TIMEOUTCHECKER_H
