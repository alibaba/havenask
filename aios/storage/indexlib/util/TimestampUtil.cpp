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
#include "indexlib/util/TimestampUtil.h"

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace util {
AUTIL_LOG_SETUP(indexlib.util, TimestampUtil);

int TimestampUtil::_days[12] = {31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
int TimestampUtil::_lpDays[12] = {31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};
int TimestampUtil::_dayToMonth[366] = {
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
    6,  6,  6,  6,  6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
    9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 0};
int TimestampUtil::_lpDayToMonth[366] = {
    0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
    0,  0,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    1,  1,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,  2,
    2,  2,  2,  2,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,  3,
    3,  3,  3,  3,  3,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    4,  4,  4,  4,  4,  4,  4,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,  5,
    5,  5,  5,  5,  5,  5,  5,  5,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  6,
    6,  6,  6,  6,  6,  6,  6,  6,  6,  6,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,
    7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  7,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,
    8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  8,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,
    9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  9,  10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11,
    11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11};

TimestampUtil::TimestampUtil() {}

TimestampUtil::~TimestampUtil() {}

TimestampUtil::TM TimestampUtil::ConvertToTM(uint64_t timp)
{
    if (unlikely(timp > _MAX_TIME_SUPPORT)) {
        AUTIL_LOG(WARN, "timestamp [%lu] over limit [%lu], will align to 2100-01-01", timp, _MAX_TIME_SUPPORT);
        timp = _MAX_TIME_SUPPORT;
    }
    uint64_t caltim = timp / 1000; /* = *timp; */ /* calendar time to convert */
    int islpyr = 0;                               /* is-current-year-a-leap-year flag */
    TM result;
    result.millisecond = timp % 1000;
    /*
     * Determine years since 1970. First, identify the four-year interval
     * since this makes handling leap-years easy (note that 2000 IS a
     * leap year and 2100 is out-of-range).
     */
    int tmptim = (int)(caltim / _FOUR_YEAR_SEC);
    caltim -= ((uint64_t)tmptim * _FOUR_YEAR_SEC);
    int* mdays = NULL;
    int* mydays = NULL;
    /*
     * Determine which year of the interval
     */
    tmptim = (tmptim * 4); /* 1970, 1974, 1978,...,etc. */
    if (caltim >= _YEAR_SEC) {
        tmptim++; /* 1971, 1975, 1979,...,etc. */
        caltim -= _YEAR_SEC;
        if (caltim >= _YEAR_SEC) {
            tmptim++; /* 1972, 1976, 1980,...,etc. */
            caltim -= _YEAR_SEC;
            /*
             * Note, it takes 366 days-worth of seconds to get past a leap
             * year.
             */
            if (caltim >= (_YEAR_SEC + _DAY_SEC)) {
                tmptim++; /* 1973, 1977, 1981,...,etc. */
                caltim -= (_YEAR_SEC + _DAY_SEC);
            } else {
                /*
                 * In a leap year after all, set the flag.
                 */
                islpyr++;
            }
        }
    }
    /*
     * tmptim now holds the value for tm_year. caltim now holds the
     * number of elapsed seconds since the beginning of that year.
     */
    result.year = tmptim;
    /*
     * Determine days since January 1 (0 - 365). This is the tm_yday value.
     * Leave caltim with number of elapsed seconds in that day.
     */
    int tm_yday = (int)(caltim / _DAY_SEC);
    caltim -= (uint64_t)(tm_yday)*_DAY_SEC;

    /*
     * Determine months since January (0 - 11) and day of month (1 - 31)
     */
    if (islpyr) {
        mdays = _lpDays;
        mydays = _lpDayToMonth;
    } else {
        mdays = _days;
        mydays = _dayToMonth;
    }

    result.month = mydays[tm_yday];

    result.day = tm_yday - (result.month ? mdays[result.month - 1] : 0);
    result.hour = (int)(caltim / 3600);
    caltim -= result.hour * 3600LL;
    result.day++;
    result.month++;
    result.minute = caltim / 60;
    result.second = caltim - (int)result.minute * 60;
    return result;
}

uint64_t TimestampUtil::ConvertToTimestamp(const TM& tm)
{
    if (tm.year > 130) {
        INDEXLIB_FATAL_ERROR(UnSupported, "year [%lu] over time limit 2100", (uint64_t)(tm.year + 1970));
    }
    bool isLeapYear = ((tm.year + 2) % 4 == 0);

    int* days = isLeapYear ? _lpDays : _days;
    assert(tm.month >= 1 && tm.month <= 12);
    uint64_t passMonDays = (tm.month <= 1) ? 0 : days[tm.month - 2];
    uint64_t leapModifyDays = (tm.year + 1) >> 2;

    assert(tm.hour >= 0 && tm.hour < 24);
    assert(tm.minute >= 0 && tm.minute < 60);
    assert(tm.second >= 0 && tm.second < 60);
    assert(tm.millisecond >= 0 && tm.millisecond < 1000);

    uint64_t inDaySec = tm.hour * 3600 + tm.minute * 60 + tm.second;
    uint64_t passYearSecs = tm.year * _YEAR_SEC;
    uint64_t passDaySec = (passMonDays + leapModifyDays + tm.day - 1) * _DAY_SEC;
    return (passYearSecs + passDaySec + inDaySec) * 1000 + tm.millisecond;
}

uint64_t TimestampUtil::ToTimestamp(uint64_t adYear, uint64_t month, uint64_t day, uint64_t hour, uint64_t min,
                                    uint64_t seconds, uint64_t millonSec, int64_t timeZoneDelta)
{
    if (unlikely(adYear < 1970 || adYear >= 2100 || month == 0 || day == 0 || hour > 23 || min > 59 || seconds > 59 ||
                 millonSec > 999)) {
        AUTIL_LOG(WARN, "unsupport timestamp format [%04lu-%02lu-%02lu %02lu:%02lu:%02lu.%03lu]", adYear, month, day,
                  hour, min, seconds, millonSec);
        return 0;
    }

    TM tm;
    tm.year = adYear - 1970;
    tm.month = month;
    tm.day = day;
    tm.hour = hour;
    tm.minute = min;
    tm.second = seconds;
    tm.millisecond = millonSec;

    int64_t ret = (int64_t)TimestampUtil::ConvertToTimestamp(tm) + timeZoneDelta;
    if (ret < 0) {
        return 0;
    }
    if ((uint64_t)ret > _MAX_TIME_SUPPORT) {
        return _MAX_TIME_SUPPORT;
    }
    return (uint64_t)ret;
}

uint64_t TimestampUtil::TimeToTimestamp(uint64_t hour, uint64_t min, uint64_t seconds, uint64_t millonSec)
{
    if (unlikely(hour > 23 || min > 59 || seconds > 59 || millonSec > 999)) {
        AUTIL_LOG(WARN, "invalid time format [%02lu:%02lu:%02lu.%03lu]", hour, min, seconds, millonSec);
        return 0;
    }

    TM tm;
    tm.year = 0;
    tm.month = 1;
    tm.day = 1;
    tm.hour = hour;
    tm.minute = min;
    tm.second = seconds;
    tm.millisecond = millonSec;
    return TimestampUtil::ConvertToTimestamp(tm);
}

uint64_t TimestampUtil::DateToTimestamp(uint64_t adYear, uint64_t month, uint64_t day)
{
    if (unlikely(adYear < 1970 || adYear >= 2100 || month == 0 || day == 0)) {
        AUTIL_LOG(WARN, "unsupport date format [%04lu-%02lu-%02lu]", adYear, month, day);
        return 0;
    }

    TM tm;
    tm.year = adYear - 1970;
    tm.month = month;
    tm.day = day;
    tm.hour = 0;
    tm.minute = 0;
    tm.second = 0;
    tm.millisecond = 0;
    return TimestampUtil::ConvertToTimestamp(tm);
}

bool TimestampUtil::ParseDate(const StringView& str, uint32_t& year, uint32_t& month, uint32_t& day)
{
    vector<StringView> dateInfo;
    dateInfo.reserve(3);

    StringTokenizer::tokenize(str, "-", dateInfo);
    if (dateInfo.size() != 3) {
        return false;
    }

    if (!StrToUInt32(dateInfo[0], year) || !StrToUInt32(dateInfo[1], month) || !StrToUInt32(dateInfo[2], day)) {
        return false;
    }
    return year >= 1970 && year < 2100 && month > 0 && month <= 12 && day > 0 && day <= 31;
}

bool TimestampUtil::ParseTime(const StringView& str, uint32_t& hour, uint32_t& minute, uint32_t& second,
                              uint32_t& millisecond)
{
    vector<StringView> timeInfo;
    timeInfo.reserve(3);

    StringTokenizer::tokenize(str, ":", timeInfo);
    if (timeInfo.size() != 3) {
        return false;
    }

    if (!StrToUInt32(timeInfo[0], hour) || !StrToUInt32(timeInfo[1], minute)) {
        return false;
    }

    vector<StringView> tmp;
    tmp.reserve(2);
    StringTokenizer::tokenize(timeInfo[2], ".", tmp);
    if (tmp.size() > 2) {
        return false;
    }

    if (!StrToUInt32(tmp[0], second)) {
        return false;
    }

    millisecond = 0;
    if (tmp.size() > 1 && !StrToUInt32(tmp[1], millisecond)) {
        return false;
    }
    return hour < 24 && minute < 60 && second < 60 && millisecond < 1000;
}

bool TimestampUtil::ParseTimestamp(const StringView& str, uint32_t& year, uint32_t& month, uint32_t& day,
                                   uint32_t& hour, uint32_t& minute, uint32_t& second, uint32_t& millisecond,
                                   int32_t& timeZoneDelta, int32_t defaultTimeZoneDelta)
{
    timeZoneDelta = defaultTimeZoneDelta;
    vector<StringView> tsInfo;
    tsInfo.reserve(3);

    StringTokenizer::tokenize(str, " ", tsInfo);
    if (tsInfo.size() > 3 || tsInfo.size() < 2) {
        return false;
    }

    if (!ParseDate(tsInfo[0], year, month, day) || !ParseTime(tsInfo[1], hour, minute, second, millisecond)) {
        return false;
    }
    return tsInfo.size() == 2 || ParseTimeZone(tsInfo[2], timeZoneDelta);
}

bool TimestampUtil::ParseTimeZone(const StringView& str, int32_t& timeZoneDelta)
{
    if (str.size() != 3 && str.size() != 5) {
        return false;
    }

    if (*str.begin() != '+' && *str.begin() != '-') {
        return false;
    }

    uint32_t number = 0;
    StringView tmp(str.data() + 1, str.size() - 1);
    if (!StrToUInt32(tmp, number)) {
        return false;
    }

    uint32_t hour = 0;
    uint32_t minute = 0;
    if (str.size() == 5) {
        minute = number % 100;
        number /= 100;
    }
    hour = number;
    if (hour > 14 || minute >= 60) {
        return false;
    }

    timeZoneDelta = 0;
    if (*str.begin() == '+') {
        timeZoneDelta -= (hour * 3600 + minute * 60) * 1000;
    } else {
        timeZoneDelta += (hour * 3600 + minute * 60) * 1000;
    }
    return true;
}

string TimestampUtil::TimeZoneDeltaToString(int32_t timeZoneDelta)
{
    char buffer[10] = {0};

    int32_t minute = 0;
    if (timeZoneDelta <= 0) {
        minute = (0 - timeZoneDelta) / 1000 / 60;
        buffer[0] = '+';
    } else {
        minute = timeZoneDelta / 1000 / 60;
        buffer[0] = '-';
    }
    snprintf(buffer + 1, sizeof(buffer) - 1, "%02d%02d", minute / 60, minute % 60);
    return string(buffer);
}

bool TimestampUtil::StrToUInt32(const StringView& str, uint32_t& value)
{
    if (str.size() >= 128) {
        return false;
    }

    char buffer[129];
    memcpy(buffer, str.data(), str.size());
    buffer[str.size()] = '\0';
    return StringUtil::strToUInt32(buffer, value);
}

bool TimestampUtil::ConvertToTimestamp(FieldType ft, const StringView& str, uint64_t& value,
                                       int32_t defaultTimeZoneDelta)
{
    uint32_t year = 0;
    uint32_t mon = 0;
    uint32_t day = 0;
    uint32_t hour = 0;
    uint32_t min = 0;
    uint32_t sec = 0;
    uint32_t milSec = 0;
    int32_t timeZoneDelta = 0;
    switch (ft) {
    case ft_date: {
        assert(defaultTimeZoneDelta == 0);
        if (!ParseDate(str, year, mon, day)) {
            return false;
        }
        value = DateToTimestamp(year, mon, day);
        return true;
    }

    case ft_time: {
        assert(defaultTimeZoneDelta == 0);
        if (!ParseTime(str, hour, min, sec, milSec)) {
            return false;
        }
        value = TimeToTimestamp(hour, min, sec, milSec);
        return true;
    }
    case ft_timestamp: {
        if (!ParseTimestamp(str, year, mon, day, hour, min, sec, milSec, timeZoneDelta, defaultTimeZoneDelta)) {
            return false;
        }
        value = ToTimestamp(year, mon, day, hour, min, sec, milSec, timeZoneDelta);
        return true;
    }
    case ft_uint64:
        return StringUtil::strToUInt64(str.data(), value);
    default:
        assert(false);
    }
    return false;
}

string TimestampUtil::TimestampToString(uint64_t timestamp)
{
    TM tm = ConvertToTM(timestamp);
    char buffer[128];
    snprintf(buffer, 128, "%04lu-%02lu-%02lu %02lu:%02lu:%02lu.%03lu", (uint64_t)tm.GetADYear(), (uint64_t)tm.month,
             (uint64_t)tm.day, (uint64_t)tm.hour, (uint64_t)tm.minute, (uint64_t)tm.second, (uint64_t)tm.millisecond);
    return string(buffer);
}

string TimestampUtil::TimeToString(uint32_t time)
{
    TM tm = ConvertToTM(time);
    char buffer[128];
    snprintf(buffer, 128, "%02lu:%02lu:%02lu.%03lu", (uint64_t)tm.hour, (uint64_t)tm.minute, (uint64_t)tm.second,
             (uint64_t)tm.millisecond);
    return string(buffer);
}

// dateIdx : current date index from 1970-01-01
string TimestampUtil::DateIndexToString(uint32_t dateIdx)
{
    TM tm = ConvertToTM(DateIndexToTimestamp(dateIdx));
    char buffer[128];
    snprintf(buffer, 128, "%04lu-%02lu-%02lu", (uint64_t)tm.GetADYear(), (uint64_t)tm.month, (uint64_t)tm.day);
    return string(buffer);
}
}} // namespace indexlib::util
