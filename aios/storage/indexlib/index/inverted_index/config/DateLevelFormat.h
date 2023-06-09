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

#include <cstdint>

#include "autil/Log.h"

namespace indexlib::config {

class DateLevelFormat
{
private:
    static const uint16_t DEFALT_LEVEL_FORMAT = 4095;

public:
    DateLevelFormat() : _levelFormat(DEFALT_LEVEL_FORMAT) {}
    ~DateLevelFormat();

public:
    enum Granularity { GU_UNKOWN, MILLISECOND, SECOND, MINUTE, HOUR, DAY, MONTH, YEAR };
    void Init(Granularity buildGranularity);
    uint16_t GetLevelFormatNumber() const { return _levelFormat; }
    bool HasLevel(uint64_t level) const { return _levelFormat >> (level - 1) & 1; }
    bool HasMillisecond() const { return _levelFormat & 1; }
    bool HasMiddleSecond() const { return _levelFormat >> 1 & 1; }
    bool HasSecond() const { return _levelFormat >> 2 & 1; }
    bool HasMiddleMinute() const { return _levelFormat >> 3 & 1; }
    bool HasMinute() const { return _levelFormat >> 4 & 1; }
    bool HasMiddleHour() const { return _levelFormat >> 5 & 1; }
    bool HasHour() const { return _levelFormat >> 6 & 1; }
    bool HasMiddleDay() const { return _levelFormat >> 7 & 1; }
    bool HasDay() const { return _levelFormat >> 8 & 1; }
    bool HasMiddleMonth() const { return _levelFormat >> 9 & 1; }
    bool HasMonth() const { return _levelFormat >> 10 & 1; }
    bool HasYear() const { return _levelFormat >> 11 & 1; }

    static uint64_t GetGranularityLevel(Granularity granularity);
    bool operator==(const DateLevelFormat& other) const { return _levelFormat == other._levelFormat; }

public:
    // only for test
    void Init(uint16_t levelFormat, Granularity buildGranularity)
    {
        _levelFormat = levelFormat;
        RemoveLowerGranularity(buildGranularity);
    }

private:
    void RemoveLowerGranularity(Granularity granularity);

private:
    uint16_t _levelFormat;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::config
