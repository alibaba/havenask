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

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/common/Common.h"

namespace swift {
namespace util {

class TimeoutChecker {
public:
    static const int64_t DEFALUT_EXPIRE_TIMEOUT = (int64_t)10 * 1000 * 1000; // 10s
public:
    TimeoutChecker(int64_t beginTime = autil::TimeUtility::currentTime());
    ~TimeoutChecker();

public:
    bool isTimeout() const { return _isTimeout; }
    bool checkTimeout() {
        if (!isTimeout()) {
            int64_t currentTime = autil::TimeUtility::currentTime();
            _isTimeout = currentTime >= _expireTime;
        }
        return isTimeout();
    }
    bool setTimeout(bool timeout) {
        _isTimeout = timeout;
        return isTimeout();
    }

    void setExpireTimeout(int64_t timeout) {
        _expireTime = _beginTime + timeout;
        _isTimeout = false;
    }

    int64_t getExpireTimeout() const { return _expireTime; }
    int64_t getBeginTime() const { return _beginTime; }

private:
    TimeoutChecker(const TimeoutChecker &);
    TimeoutChecker &operator=(const TimeoutChecker &);

public:
private:
    bool _isTimeout;
    int64_t _beginTime;
    int64_t _expireTime;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TimeoutChecker);

} // namespace util
} // namespace swift
