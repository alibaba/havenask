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

#include <stdint.h>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"

namespace autil {

class TimeoutTerminator {
public:
    // default construct means never timeout
    TimeoutTerminator();
    TimeoutTerminator(int64_t timeoutInUs);
    TimeoutTerminator(int64_t timeoutInUs, int64_t startTimeInUs);
    virtual ~TimeoutTerminator();

public:
    // reset check count, set check step.
    // checkStep must be 2^n
    void init(int32_t checkStep);
    void init(int32_t checkStep, int32_t restrictStep);

    bool checkTimeout() { return checkTimeout(_checkMask, _checkTimes); }

    bool checkRestrictTimeout() { return checkTimeout(_restrictorCheckMask, _restrictorCheckTimes); }
    int32_t getCheckTimes() const;
    int32_t getRestrictorCheckTimes() const;
    int64_t getLeftTime();
    int64_t getExpireTime() const;
    int64_t getStartTime() const;
    int64_t getTimeout() const;
    bool hasTimeout() const;
    // call checkTimeout or checkRestrictTimeout to update is timeout
    inline bool isTimeout() const { return _isTimeout; }
    inline bool isTerminated() const { return _isTerminated; }
    inline void setTerminated(bool isTerminated) { _isTerminated = isTerminated; }
    void updateExpireTime(int64_t expireTimeInUs);
    virtual int64_t getCurrentTime() { return autil::TimeUtility::currentTime(); }

public:
    static const int32_t DEFAULT_RESTRICTOR_CHECK_MASK = 15;

private:
    inline bool checkTimeout(int32_t checkMask, int32_t &checkTimes) {
        bool needCheck = (checkTimes++ & checkMask) == 0;
        if (unlikely(needCheck)) {
            int64_t currentTime = getCurrentTime();
            if (unlikely(currentTime >= _expireTime)) {
                _isTimeout = true;
                _isTerminated = true;
            }
        }
        return isTerminated();
    }


private:
    int64_t _timeout;
    int64_t _expireTime;
    int64_t _startTime;
    int32_t _checkMask;
    int32_t _checkTimes;
    int32_t _restrictorCheckMask;
    int32_t _restrictorCheckTimes;
    bool _isTimeout;
    bool _isTerminated;
};

typedef std::shared_ptr<TimeoutTerminator> TimeoutTerminatorPtr;

} // namespace autil
