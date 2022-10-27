#ifndef __INDEXLIB_DATE_TERM_H
#define __INDEXLIB_DATE_TERM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/timestamp_util.h"
#include "indexlib/config/date_level_format.h"

IE_NAMESPACE_BEGIN(common);

class DateTerm
{
public:
    typedef std::pair<uint64_t, uint64_t> Range;
    typedef std::vector<Range> Ranges;

public:
    DateTerm(uint64_t term = 0);
    ~DateTerm();

    // for ut
    DateTerm(uint64_t year, uint64_t month, uint64_t day,
             uint64_t hour, uint64_t minute, uint64_t second,
             uint64_t millisecond = 0);
public:
    void SetMillisecond(uint64_t value)
    {
        mDateTerm &= ~((1LL << 10) - 1);
        mDateTerm |= value;
    }
    void SetSecond(uint64_t value)
    {
        mDateTerm &= ~(((1LL << 6) - 1) << 10);
        mDateTerm |= value << 10;
    }
    void SetMinute(uint64_t value)
    {
        mDateTerm &= ~(((1LL << 6) - 1) << 16);
        mDateTerm |= value << 16;
    }
    void SetHour(uint64_t value)
    {
        mDateTerm &= ~(((1LL << 5) - 1) << 22);
        mDateTerm |= value << 22;
    }
    void SetDay(uint64_t value)
    {
        mDateTerm &= ~(((1LL << 5) - 1) << 27);
        mDateTerm |= value << 27;
    }
    void SetMonth(uint64_t value)
    {
        mDateTerm &= ~(((1LL << 4) - 1) << 32);
        mDateTerm |= value << 32;
    }
    void SetYear(uint64_t value)
    {
        mDateTerm &= ~(((1LL << 9) - 1) << 36);
        mDateTerm |= value << 36;
    }
    void SetLevelType(uint64_t value)
    {
        mDateTerm &= (1LL << 60) - 1;
        mDateTerm |= value << 60;
    }
    uint64_t GetLevel() const
    {
        return mDateTerm >> 60;
    }
    void PlusUnit(uint64_t unit) { mDateTerm += unit; }
    void SubUnit(uint64_t unit) { mDateTerm -= unit; }
    dictkey_t GetKey() { return mDateTerm; }

    bool operator<(const DateTerm& other) const
    {
        return mDateTerm < other.mDateTerm;
    }
    bool operator==(const DateTerm& other) const
    {
        return mDateTerm == other.mDateTerm;
    }
public:
    static uint64_t NormalizeToNextYear(uint64_t key)
    {
        return ((key >> 36) + 1)<< 36;
    }

    static uint64_t NormalizeToYear(uint64_t key)
    {
        return (key >> 36)<< 36;
    }
    static std::string ToString(DateTerm dateterm);
    static DateTerm FromString(const std::string& content);    
    static void CalculateTerms(uint64_t leftDateTerm, uint64_t rightDateTerm,
                               const config::DateLevelFormat& format,
                               std::vector<uint64_t>& terms);
    static void CalculateRanges(uint64_t leftTerm, uint64_t rightTerm,
                                const config::DateLevelFormat& format, Ranges& ranges);
    static void EncodeDateTermToTerms(DateTerm dateTerm,
            const config::DateLevelFormat& format,
            std::vector<dictkey_t>& dictKeys);
    static void AddTerms(uint64_t fromKey, uint64_t toKey, uint64_t level,
                         uint64_t totalBits, std::vector<uint64_t>& terms);
    static uint64_t GetLevelBit(uint8_t level, const config::DateLevelFormat& format);
    static uint64_t GetSearchGranularityUnit(config::DateLevelFormat::Granularity granularity);
    static DateTerm ConvertToDateTerm(util::TimestampUtil::Date date,
            config::DateLevelFormat format);
public:

private:
    dictkey_t mDateTerm;
    static uint64_t mLevelBits[13];
    static uint64_t mNoMiddleLevelBits[13];
    static uint64_t mLevelValueLimits[13];

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateTerm);

//////////////////////////////////////////////

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DATE_TERM_H
