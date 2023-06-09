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
/**
* @file     Clock.h
* @brief    Utility class to get the current time of different precision.
*
* @version  1.0.0
* @date     2013.1.12
* @author   yuejun.huyj
*/

#ifndef __ALOG_CLOCK_H__
#define __ALOG_CLOCK_H__

#include <sys/time.h>
#include <stdint.h>

namespace alog
{

class Clock
{
public:
    Clock() {};
    ~Clock() {};

    // Get the current time in miliseconds
    static uint64_t nowMS()
    {
        struct timeval tv = {0};
        gettimeofday (&tv, NULL);
        return (tv.tv_sec * (uint64_t) 1000 + tv.tv_usec / 1000);
    }

    // Get the current time in microseconds
    static uint64_t nowUS()
    {
        struct timeval tv = {0};
        gettimeofday (&tv, NULL);
        return (tv.tv_sec * (uint64_t) 1000000 + tv.tv_usec);
    }

private:
    Clock(const Clock&);
    Clock &operator = (const Clock&);
};

}

#endif
