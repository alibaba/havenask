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
#include <sys/time.h>
#include <time.h>
#include <assert.h>
#include <string>
#include <limits>
#include <algorithm>

namespace autil {

template<typename T>
float US_TO_MS(T us) {
    return us / 1000.0;
}
template<typename T>
float US_TO_SEC(T us) {
    return us / 1000.0 / 1000.0;
}

inline int64_t TIME_ADD(int64_t startTime, int64_t diff) {
    assert(startTime >= 0);
    // avoid op plus overflow
    int64_t timeoutThreshold = std::numeric_limits<int64_t>::max() - startTime;
    diff = std::min(diff, timeoutThreshold);
    return startTime + diff;
}

class TimeUtility
{
public:
    static int64_t currentTime(); // us
    static int64_t currentTimeInSeconds();
    static int64_t currentTimeInMilliSeconds(); // ms
    static int64_t currentTimeInMicroSeconds(); // us
    static int64_t currentTimeInNanoSeconds(); // ns
    static int64_t monotonicTimeUs();
    static int64_t monotonicTimeNs();

    static int64_t getTime(int64_t usecOffset = 0);
    static timeval getTimeval(int64_t usecOffset = 0);
    static timespec getTimespec(int64_t usecOffset = 0);
    static int64_t us2ms(int64_t us) {
        return us / 1000;
    }
    static int64_t ms2us(int64_t ms) {
        return ms * 1000;
    }
    static int64_t ms2sec(int64_t ms) {
        return ms / 1000;
    }
    static int64_t sec2ms(int64_t sec) {
        return sec * 1000;
    }
    static int64_t us2sec(int64_t us) {
        return us / 1000000;
    }
    static int64_t sec2us(int64_t sec) {
        return sec * 1000000;
    }
    static std::string currentTimeString();
    static std::string currentTimeString(const std::string &format);
};

class ScopedTime {
public:
    ScopedTime(int64_t &t) : _t(t) {
        _b = TimeUtility::monotonicTimeUs();
    }
    ~ScopedTime() {
        _t += TimeUtility::monotonicTimeUs() - _b;
    }
private:
    int64_t &_t;
    int64_t _b;
};

class ScopedTime2 {
public:
    ScopedTime2() {
        _beginTime = TimeUtility::monotonicTimeUs();
    }
    ~ScopedTime2() {
    }
    double done_sec() const {
        int64_t endTime = TimeUtility::monotonicTimeUs();
        return US_TO_SEC(endTime - _beginTime);
    }
    double done_ms() const {
        int64_t endTime = TimeUtility::monotonicTimeUs();
        return US_TO_MS(endTime - _beginTime);
    }
    int64_t done_us() const {
        int64_t endTime = TimeUtility::monotonicTimeUs();
        return endTime - _beginTime;
    }
    int64_t begin_us() const {
        return TimeUtility::currentTime() - done_us();
    }

private:
    int64_t _beginTime;
};

class StageTime {
public:
    StageTime() {
        _beginTime = TimeUtility::monotonicTimeUs();
    }
    ~StageTime() = default;
    void end_stage() {
        int64_t endTime = TimeUtility::monotonicTimeUs();
        _lastStageUs = endTime - _beginTime;
        _beginTime = endTime;
    }
    double last_sec() const {
        return US_TO_SEC(last_us());
    }
    double last_ms() const {
        return US_TO_MS(last_us());
    }
    int64_t last_us() const {
        return _lastStageUs;
    }
    
private:
    int64_t _beginTime;
    int64_t _lastStageUs;
};

}

#define AUTIL_TIMEUTILITY_RECORD_CODE_TIME(code) ({                       \
    const auto startTime = autil::TimeUtility::currentTime();             \
    code;                                                                 \
    const auto latency = autil::TimeUtility::currentTime() - startTime;   \
    latency;                                                              \
})
