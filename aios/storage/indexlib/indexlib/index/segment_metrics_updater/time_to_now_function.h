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

#include <cassert>
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/segment_metrics_updater/match_function.h"
#include "indexlib/util/KeyValueMap.h"
#include "indexlib/util/Timer.h"

namespace indexlib { namespace index {

template <typename T1, typename T2>
class TimeToNowFunction : public MatchFunction
{
private:
    enum TimeUnit {
        tu_second,
        tu_millisecond,
    };

public:
    TimeToNowFunction() : mTimeUnit(tu_second) {}
    ~TimeToNowFunction() {}

public:
    virtual bool Init(util::KeyValueMap param) override;
    virtual void Execute(int8_t* inputValue, int8_t* outputValue) override;

private:
    TimeUnit mTimeUnit;

private:
    IE_LOG_DECLARE();
};

///////////////////////////

template <typename T1, typename T2>
bool TimeToNowFunction<T1, T2>::Init(util::KeyValueMap param)
{
    std::string timeUnit = util::GetValueFromKeyValueMap(param, "time_unit");
    if (timeUnit == "millisecond") {
        mTimeUnit = tu_millisecond;
    }
    return true;
}

template <typename T1, typename T2>
void TimeToNowFunction<T1, T2>::Execute(int8_t* inputValue, int8_t* outputValue)
{
    T2& value = *(T2*)(outputValue);
    switch (mTimeUnit) {
    case tu_second:
        value = util::Timer::GetCurrentTime() - *(T1*)(inputValue);
        break;
    case tu_millisecond:
        value = util::Timer::GetCurrentTimeInMilliSeconds() - *(T1*)(inputValue);
        break;
    default:
        assert(false);
    }
}
}} // namespace indexlib::index
