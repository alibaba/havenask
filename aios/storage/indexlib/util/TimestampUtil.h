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
#include <string>

#include "autil/Log.h"
#include "autil/Span.h"
#include "indexlib/base/FieldType.h"
#include "indexlib/util/TimestampUtilDefine.h"

namespace indexlib { namespace util {

class TimestampUtil
{
public:
    typedef TM_Structure TM;
    static const int64_t DAY_MILLION_SEC = 86400 * 1000;

public:
    TimestampUtil();
    ~TimestampUtil();

public:
    static TM ConvertToTM(uint64_t miliseconds);
    static uint64_t ConvertToTimestamp(const TM& tm);

public:
    static bool ConvertToTimestamp(FieldType ft, const autil::StringView& str, uint64_t& value,
                                   int32_t defaultTimeZoneDelta);

    static bool ParseTimeZone(const autil::StringView& str, int32_t& timeZoneDelta);

    static std::string TimeZoneDeltaToString(int32_t timeZoneDelta);

    static std::string TimestampToString(uint64_t timestamp);

    // return XX:XX:XX.XXX (hour:minute:second.millionSeconds)
    static std::string TimeToString(uint32_t time);

    // return XXXX-XX-XX (year-month-day)
    // dateIdx : current date index from 1970-01-01
    static std::string DateIndexToString(uint32_t dateIdx);

    static uint64_t DateIndexToTimestamp(uint32_t dateIdx) { return dateIdx * DAY_MILLION_SEC; }

private:
    static bool StrToUInt32(const autil::StringView& str, uint32_t& value);
    static bool ParseTimestamp(const autil::StringView& str, uint32_t& year, uint32_t& month, uint32_t& day,
                               uint32_t& hour, uint32_t& minute, uint32_t& second, uint32_t& millisecond,
                               int32_t& timeZoneDelta, int32_t defaultTimeZoneDelta);

    static bool ParseDate(const autil::StringView& str, uint32_t& year, uint32_t& month, uint32_t& day);

    static bool ParseTime(const autil::StringView& str, uint32_t& hour, uint32_t& minute, uint32_t& second,
                          uint32_t& millisecond);

    static uint64_t ToTimestamp(uint64_t adYear, uint64_t month, uint64_t day, uint64_t hour, uint64_t min,
                                uint64_t seconds, uint64_t millonSec, int64_t timeZoneDelta);

    static uint64_t TimeToTimestamp(uint64_t hour, uint64_t min, uint64_t seconds, uint64_t millonSec);

    static uint64_t DateToTimestamp(uint64_t adYear, uint64_t month, uint64_t day);

private:
    static const int64_t _FOUR_YEAR_SEC = 126230400;
    static const int64_t _YEAR_SEC = 31536000;
    static const int64_t _DAY_SEC = 86400;
    static const int64_t _MAX_TIME_SUPPORT = 4102444800000; // 2100-01-01 00:00:00
    static int _days[12];
    static int _lpDays[12];
    static int _dayToMonth[366];
    static int _lpDayToMonth[366];

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::util
