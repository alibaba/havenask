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

#include <memory>

namespace indexlib { namespace util {

class Timer
{
public:
    Timer(bool autoStart = true)
    {
        if (autoStart) {
            Start();
        }
    }

    void Start() { clock_gettime(CLOCK_REALTIME, &startTime); }

    void RestartTimer() { Start(); }

    int64_t Stop()
    {
        struct timespec stopTime;
        clock_gettime(CLOCK_REALTIME, &stopTime);
        return (int64_t)(stopTime.tv_sec - startTime.tv_sec) * 1000000 + (stopTime.tv_nsec - startTime.tv_nsec) / 1000;
    }

    static uint64_t GetCurrentTimeInNanoSeconds()
    {
        struct timespec time;
        clock_gettime(CLOCK_REALTIME, &time);
        uint64_t t = static_cast<uint64_t>(time.tv_sec);
        t *= 1000000000; // Convert seconds to nano-seconds
        t += static_cast<uint64_t>(time.tv_nsec);
        return t;
    }

    static uint64_t GetCurrentTimeInMicroSeconds()
    {
        struct timespec time;
        clock_gettime(CLOCK_REALTIME, &time);
        uint64_t t = static_cast<uint64_t>(time.tv_sec);
        t *= 1000000; // Convert seconds to micro-seconds
        t += static_cast<uint64_t>(time.tv_nsec) / 1000;
        return t;
    }

    static uint64_t GetCurrentTimeInMilliSeconds()
    {
        struct timespec time;
        clock_gettime(CLOCK_REALTIME, &time);
        uint64_t t = static_cast<uint64_t>(time.tv_sec);
        t *= 1000; // Convert seconds to milli-seconds
        t += static_cast<uint64_t>(time.tv_nsec) / 1000000;
        return t;
    }

    static uint64_t GetCurrentTime()
    {
        struct timespec time;
        clock_gettime(CLOCK_REALTIME, &time);
        return static_cast<uint64_t>(time.tv_sec);
    }

    static std::string GetCurrentTimeStr()
    {
        time_t t = GetCurrentTime();
        char buffer[100];
        ctime_r(&t, buffer);
        return std::string(buffer);
    }

    static std::string GetCurrentTimeStamp(const std::string& format = "%Y%m%d%H%M%S")
    {
        time_t t = GetCurrentTime();
        char buffer[100];
        struct tm timeinfo;
        localtime_r(&t, &timeinfo);
        strftime(buffer, 100, format.c_str(), &timeinfo);
        return std::string(buffer);
    }

    static std::string GetTimeStamp(const time_t& tm)
    {
        char buffer[100];
        return std::string(ctime_r(&tm, buffer));
    }

    static std::string ConvertToTimeStamp(const time_t& t, const std::string& format = "%Y%m%d%H%M%S")
    {
        char buffer[100];
        struct tm timeinfo;
        localtime_r(&t, &timeinfo);
        strftime(buffer, 100, format.c_str(), &timeinfo);
        return std::string(buffer);
    }

private:
    struct timespec startTime;
};
typedef std::shared_ptr<Timer> TimerPtr;
}} // namespace indexlib::util
