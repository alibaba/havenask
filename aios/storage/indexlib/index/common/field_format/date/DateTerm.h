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

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/config/DateLevelFormat.h"
#include "indexlib/util/TimestampUtil.h"

namespace indexlib::index {

class DateTerm
{
public:
    typedef std::pair<uint64_t, uint64_t> Range;
    typedef std::vector<Range> Ranges;

public:
    DateTerm(uint64_t term = 0);
    ~DateTerm();

    // for ut
    DateTerm(uint64_t year, uint64_t month, uint64_t day, uint64_t hour, uint64_t minute, uint64_t second,
             uint64_t millisecond = 0);

public:
    void SetMillisecond(uint64_t value)
    {
        _dateTerm &= ~((1LL << 10) - 1);
        _dateTerm |= value;
    }
    void SetSecond(uint64_t value)
    {
        _dateTerm &= ~(((1LL << 6) - 1) << 10);
        _dateTerm |= value << 10;
    }
    void SetMinute(uint64_t value)
    {
        _dateTerm &= ~(((1LL << 6) - 1) << 16);
        _dateTerm |= value << 16;
    }
    void SetHour(uint64_t value)
    {
        _dateTerm &= ~(((1LL << 5) - 1) << 22);
        _dateTerm |= value << 22;
    }
    void SetDay(uint64_t value)
    {
        _dateTerm &= ~(((1LL << 5) - 1) << 27);
        _dateTerm |= value << 27;
    }
    void SetMonth(uint64_t value)
    {
        _dateTerm &= ~(((1LL << 4) - 1) << 32);
        _dateTerm |= value << 32;
    }
    void SetYear(uint64_t value)
    {
        _dateTerm &= ~(((1LL << 9) - 1) << 36);
        _dateTerm |= value << 36;
    }
    void SetLevelType(uint64_t value)
    {
        _dateTerm &= (1LL << 60) - 1;
        _dateTerm |= value << 60;
    }
    uint64_t GetLevel() const { return _dateTerm >> 60; }
    void PlusUnit(uint64_t unit) { _dateTerm += unit; }
    void SubUnit(uint64_t unit) { _dateTerm -= unit; }
    dictkey_t GetKey() { return _dateTerm; }

    bool operator<(const DateTerm& other) const { return _dateTerm < other._dateTerm; }
    bool operator==(const DateTerm& other) const { return _dateTerm == other._dateTerm; }

public:
    static uint64_t NormalizeToNextYear(uint64_t key) { return ((key >> 36) + 1) << 36; }

    static uint64_t NormalizeToYear(uint64_t key) { return (key >> 36) << 36; }
    static std::string ToString(DateTerm dateterm);
    static DateTerm FromString(const std::string& content);
    static void CalculateTerms(uint64_t leftDateTerm, uint64_t rightDateTerm, const config::DateLevelFormat& format,
                               std::vector<uint64_t>& terms);
    static void CalculateRanges(uint64_t leftTerm, uint64_t rightTerm, const config::DateLevelFormat& format,
                                Ranges& ranges);
    static void EncodeDateTermToTerms(DateTerm dateTerm, const config::DateLevelFormat& format,
                                      std::vector<dictkey_t>& dictKeys);
    static void AddTerms(uint64_t fromKey, uint64_t toKey, uint64_t level, uint64_t totalBits,
                         std::vector<uint64_t>& terms);
    static uint64_t GetLevelBit(uint8_t level, const config::DateLevelFormat& format);
    static uint64_t GetSearchGranularityUnit(config::DateLevelFormat::Granularity granularity);
    static DateTerm ConvertToDateTerm(util::TimestampUtil::TM date, config::DateLevelFormat format);

private:
    dictkey_t _dateTerm;
    static uint64_t _levelBits[13];
    static uint64_t _noMiddleLevelBits[13];
    static uint64_t _levelValueLimits[13];

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////

} // namespace indexlib::index
