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

#include <endian.h>
#include <stdint.h>

namespace indexlib { namespace util {

struct TM_Structure {
#if __BYTE_ORDER == __BIG_ENDIAN
    TM_Structure() : year(0), month(0), day(0), hour(0), minute(0), second(0), millisecond(0), unuse(0) {}
    // years from 1970, max valid year is 2100
    // valid range(0 ~ 130), 4 years from 1970 is leap year (include 2000)
    uint64_t year        : 9;
    uint64_t month       : 4;
    uint64_t day         : 5;
    uint64_t hour        : 5;
    uint64_t minute      : 6;
    uint64_t second      : 6;
    uint64_t millisecond : 10;
    uint64_t unuse       : 19;
#else
    TM_Structure() : unuse(0), millisecond(0), second(0), minute(0), hour(0), day(0), month(0), year(0) {}
    uint64_t unuse       : 19;
    uint64_t millisecond : 10;
    uint64_t second      : 6;
    uint64_t minute      : 6;
    uint64_t hour        : 5;
    uint64_t day         : 5;
    uint64_t month       : 4;
    uint64_t year        : 9; // years from 1970
#endif

    uint64_t GetADYear() const { return year + 1970; }
    uint64_t ToBinaryUInt64() const { return *(uint64_t*)(this); }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
    void FromBinaryUInt64(uint64_t value) { *this = *(TM_Structure*)(&value); }
#pragma GCC diagnostic pop
    static TM_Structure ToTM(uint64_t value)
    {
        TM_Structure ret;
        ret.FromBinaryUInt64(value);
        return ret;
    }

    static uint64_t ToBinary(TM_Structure tm) { return tm.ToBinaryUInt64(); }
    bool operator==(const TM_Structure& other) const { return ToBinaryUInt64() == other.ToBinaryUInt64(); }
    bool operator!=(const TM_Structure& other) const { return !(*this == other); }
    bool operator<(const TM_Structure& other) const { return ToBinaryUInt64() < other.ToBinaryUInt64(); }
    bool operator<=(const TM_Structure& other) const { return ToBinaryUInt64() <= other.ToBinaryUInt64(); }
    bool operator>=(const TM_Structure& other) const { return ToBinaryUInt64() >= other.ToBinaryUInt64(); }
    bool operator>(const TM_Structure& other) const { return ToBinaryUInt64() > other.ToBinaryUInt64(); }
};
static_assert(sizeof(TM_Structure) == 8, "invalid TM structure size");

///////////////////////////////////////////////////////////////////////////

#pragma pack(push)
#pragma pack(4)

struct Date {
#if __BYTE_ORDER == __BIG_ENDIAN
    Date() : adYear(0), month(0), day(0), unuse(0) {}

    uint32_t adYear : 12;
    uint32_t month  : 4;
    uint32_t day    : 5;
    uint32_t unuse  : 11;
#else
    Date() : unuse(0), day(0), month(0), adYear(0) {}
    uint32_t unuse  : 11;
    uint32_t day    : 5;
    uint32_t month  : 4;
    uint32_t adYear : 12;
#endif
public:
    uint32_t ToBinaryUInt32() const { return *(uint32_t*)(this); }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
    void FromBinaryUInt32(uint32_t value) { *this = *(Date*)(&value); }
#pragma GCC diagnostic pop
    static Date ToDate(uint32_t value)
    {
        Date ret;
        ret.FromBinaryUInt32(value);
        return ret;
    }

    static uint32_t ToBinary(Date other) { return other.ToBinaryUInt32(); }
    bool operator==(const Date& other) const { return ToBinaryUInt32() == other.ToBinaryUInt32(); }
    bool operator!=(const Date& other) const { return !(*this == other); }
    bool operator<(const Date& other) const { return ToBinaryUInt32() < other.ToBinaryUInt32(); }
    bool operator<=(const Date& other) const { return ToBinaryUInt32() <= other.ToBinaryUInt32(); }
    bool operator>=(const Date& other) const { return ToBinaryUInt32() >= other.ToBinaryUInt32(); }
    bool operator>(const Date& other) const { return ToBinaryUInt32() > other.ToBinaryUInt32(); }
};
#pragma pack(pop)

#pragma pack(push)
#pragma pack(4)
struct Time {
#if __BYTE_ORDER == __BIG_ENDIAN
    Time() : hour(0), minute(0), second(0), millisecond(0), unuse(0) {}
    uint32_t hour        : 5;
    uint32_t minite      : 6;
    uint32_t second      : 6;
    uint32_t millisecond : 10;
    uint32_t unuse       : 5;

#else
    Time() : unuse(0), millisecond(0), second(0), minite(0), hour(0) {}
    uint32_t unuse       : 5;
    uint32_t millisecond : 10;
    uint32_t second      : 6;
    uint32_t minite      : 6;
    uint32_t hour        : 5;

#endif
public:
    uint32_t ToBinaryUInt32() const { return *(uint32_t*)(this); }
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
    void FromBinaryUInt32(uint32_t value) { *this = *(Time*)(&value); }
#pragma GCC diagnostic pop
    static Time ToTime(uint32_t value)
    {
        Time ret;
        ret.FromBinaryUInt32(value);
        return ret;
    }

    static uint32_t ToBinary(Time other) { return other.ToBinaryUInt32(); }
    bool operator==(const Time& other) const { return ToBinaryUInt32() == other.ToBinaryUInt32(); }
    bool operator!=(const Time& other) const { return !(*this == other); }
    bool operator<(const Time& other) const { return ToBinaryUInt32() < other.ToBinaryUInt32(); }
    bool operator<=(const Time& other) const { return ToBinaryUInt32() <= other.ToBinaryUInt32(); }
    bool operator>=(const Time& other) const { return ToBinaryUInt32() >= other.ToBinaryUInt32(); }
    bool operator>(const Time& other) const { return ToBinaryUInt32() > other.ToBinaryUInt32(); }
};
#pragma pack(pop)
}} // namespace indexlib::util
