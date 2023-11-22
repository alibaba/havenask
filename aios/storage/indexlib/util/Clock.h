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

#include <chrono>
#include <stdint.h>
#include <unistd.h>

namespace indexlibv2::util {
class Clock
{
public:
    Clock() {}
    virtual ~Clock() {}

    Clock(const Clock&) = delete;
    Clock& operator=(const Clock&) = delete;
    Clock(Clock&&) = delete;
    Clock& operator=(Clock&&) = delete;

public:
    // Returns the amount of microseconds passed since epoch.
    virtual uint64_t Now()
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                   std::chrono::high_resolution_clock::now().time_since_epoch())
            .count();
    }
    // Sleep the amount of microseconds.
    virtual void SleepFor(uint64_t us) { usleep(us); }

    uint64_t NowInSeconds() { return Now() / 1000 / 1000; }
};
} // namespace indexlibv2::util
