#include "indexlib/common/field_format/date/date_term.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, DateTerm);

uint64_t DateTerm::mLevelBits[13] =          {0, 5, 5, 3, 3, 3, 3, 2, 3, 2, 3, 4, 9};
uint64_t DateTerm::mNoMiddleLevelBits[13] = {0, 10, 0, 6, 0, 6, 0, 5, 0, 5, 0, 4, 9};
uint64_t DateTerm::mLevelValueLimits[13] = {0, 999, 31, 59, 8, 59, 8, 23, 5, 31, 7, 12, 512};
DateTerm::DateTerm(uint64_t term)
    : mDateTerm(term)
{
}

DateTerm::~DateTerm()
{
}

DateTerm::DateTerm(uint64_t year, uint64_t month, uint64_t day,
                   uint64_t hour, uint64_t minute, uint64_t second,
                   uint64_t millisecond)
    : mDateTerm(0)
{
    mDateTerm |= year << 36;
    mDateTerm |= month << 32;
    mDateTerm |= day << 27;
    mDateTerm |= hour << 22;
    mDateTerm |= minute << 16;
    mDateTerm |= second << 10;
    mDateTerm |= millisecond;
}

string DateTerm::ToString(DateTerm term)
{
    uint64_t value = term.GetKey() & ((1LL << 60) - 1);
    return StringUtil::toString(value >> 36) + "-"
        + StringUtil::toString(value >> 32 & 15) + "-"
        + StringUtil::toString(value >> 27 & 31) + "-"
        + StringUtil::toString(value >> 22 & 31) + "-"
        + StringUtil::toString(value >> 16 & 63) + "-"
        + StringUtil::toString(value >> 10 & 63) + "-"
        + StringUtil::toString(value & 1023);
}

DateTerm DateTerm::FromString(const string& content)
{
    vector<uint64_t> values;
    StringUtil::fromString(content, values, "-");
    if ((size_t)7 != values.size())
    {
        return DateTerm();
    }
    DateTerm term;
    term.SetYear(values[0]);
    term.SetMonth(values[1]);
    term.SetDay(values[2]);
    term.SetHour(values[3]);
    term.SetMinute(values[4]);
    term.SetSecond(values[5]);
    term.SetMillisecond(values[6]);
    return term;
}

uint64_t DateTerm::GetLevelBit(uint8_t level, const DateLevelFormat& format)
{
    if (level & 1 && level <= 10 && !format.HasLevel(level + 1))
    {
        return mLevelBits[level] + mLevelBits[level + 1];
    }

    if (format.HasLevel(level))
    {
        return mLevelBits[level];     
    }
    return 0;
}

void DateTerm::EncodeDateTermToTerms(DateTerm dateTerm,
                                     const DateLevelFormat& format,
                                     vector<dictkey_t>& dictKeys)
{
    uint64_t totalLevelBits = 0;
    dictkey_t key = dateTerm.mDateTerm & ((1LL << 60) - 1);
    for (size_t i = 1; i <= 12; i++)
    {
        totalLevelBits += mLevelBits[i];
        if (format.HasLevel(i))
        {
            dictKeys.push_back(key | (uint64_t)i << 60);
        }
        key &= ~((1LL << totalLevelBits) - 1);
    }
}

uint64_t DateTerm::GetSearchGranularityUnit(DateLevelFormat::Granularity granularity)
{
    switch (granularity)
    {
    case DateLevelFormat::MILLISECOND:
        return 1;
    case DateLevelFormat::SECOND:
        return 1LL << 10;
    case DateLevelFormat::MINUTE:
        return 1LL << 16;
    case DateLevelFormat::HOUR:
        return 1LL << 22;
    case DateLevelFormat::DAY:
        return 1LL << 27;
    case DateLevelFormat::MONTH:
        return 1LL << 32;
    case DateLevelFormat::YEAR:
        return 1LL << 36;
    default:
        assert(false);
    }
    return 0;
}

void DateTerm::CalculateRanges(uint64_t fromKey, uint64_t toKey,
                               const config::DateLevelFormat& format, Ranges& ranges)
{
    uint64_t totalBits = 0;
    for (size_t level = 1; level <= 12; level++)
    {
        uint64_t levelBits = GetLevelBit(level, format);
        if (!format.HasLevel(level))
        {
            totalBits += levelBits;
            continue;
        }
        uint64_t levelValueMask = ((1LL << levelBits) - 1);
        uint64_t levelBitsMask = levelValueMask << totalBits;
        if (level & 1)
        {
            levelValueMask = (1LL << mNoMiddleLevelBits[level]) - 1;
        }
        uint64_t rightLevelValue = (toKey >> totalBits) & levelValueMask;
        //000011111100000   (1111111 level bits)
        bool fromKeyHasTerms = ((fromKey & levelBitsMask) != 0);
        bool toKeyHasTerms = (rightLevelValue < mLevelValueLimits[level]
                              && (toKey & levelBitsMask) != levelBitsMask);
        uint64_t plusUnit = 1LL << (totalBits + levelBits);
        uint64_t higherLeftValue =
            (fromKeyHasTerms ? (fromKey + plusUnit) : fromKey) & (~levelBitsMask);
        uint64_t higherRightValue =
            (toKeyHasTerms ? (toKey - plusUnit) : toKey) & (~levelBitsMask);

        if (higherLeftValue > higherRightValue
            || higherLeftValue < fromKey || higherRightValue > toKey)
        {
            ranges.emplace_back(fromKey | (level << 60), toKey | (level << 60));
            break;
        }

        if (fromKeyHasTerms)
        {
            ranges.emplace_back(fromKey | (level << 60) ,
                    fromKey | levelBitsMask | (level << 60));
        }

        if (toKeyHasTerms)
        {
            ranges.emplace_back((toKey & ~levelBitsMask) | (level << 60),
                    toKey | (level << 60));
        }
        fromKey = higherLeftValue;
        toKey = higherRightValue;
        totalBits += levelBits;
    }
}

//[leftDateTerm, rightDateTerm]
void DateTerm::CalculateTerms(uint64_t fromKey, uint64_t toKey,
                              const DateLevelFormat& format,
                              vector<uint64_t>& terms)
{
    uint64_t totalBits = 0;
    for (size_t level = 1; level <= 12; level++)
    {
        uint64_t levelBits = GetLevelBit(level, format);
        if (!format.HasLevel(level))
        {
            totalBits += levelBits;
            continue;
        }

        //  00000001111 //1111 level bits
        uint64_t levelValueMask = ((1LL << levelBits) - 1);
        //  000011110000
        uint64_t levelBitsMask = levelValueMask << totalBits;
        if (level & 1)
        {
            levelValueMask = (1LL << mNoMiddleLevelBits[level]) - 1;
        }
        uint64_t rightLevelValue = (toKey >> totalBits) & levelValueMask;
        //000011111100000   (1111111 level bits)
        bool fromKeyHasTerms = ((fromKey & levelBitsMask) != 0);
        bool toKeyHasTerms = (rightLevelValue < mLevelValueLimits[level]
                              && (toKey & levelBitsMask) != levelBitsMask);
        uint64_t plusUnit = 1LL << (totalBits + levelBits);
        uint64_t higherLeftValue = (fromKeyHasTerms ? (fromKey + plusUnit) : fromKey) & (~levelBitsMask);
        uint64_t higherRightValue = (toKeyHasTerms ? (toKey - plusUnit) : toKey) & (~levelBitsMask);

        if (higherLeftValue > higherRightValue
            || higherLeftValue < fromKey || higherRightValue > toKey)
        {
            AddTerms(fromKey, toKey, level, totalBits, terms);
            break;
        }

        if (fromKeyHasTerms)
        {
            AddTerms(fromKey, fromKey | levelBitsMask, level, totalBits, terms);
        }

        if (toKeyHasTerms)
        {
            AddTerms(toKey & ~levelBitsMask, toKey, level, totalBits, terms);
        }
        fromKey = higherLeftValue;
        toKey = higherRightValue;
        totalBits += levelBits;
    }
}


void DateTerm::AddTerms(uint64_t fromKey, uint64_t toKey,
                        uint64_t level, uint64_t totalBits,
                        vector<uint64_t>& terms)
{
    uint64_t plusUnit = 1LL << totalBits;
    uint64_t levelValueMask = 0;

    // when calc middle level, use lower level number to decide filter.
    // for example: 24 hou, which should be expressed to 4 middleHour,
    // but we know there is only 0 hour, instead 24 hour, so this time should be filtered.
    // so we shall take out 24 from the fromKey, instead 4 middleHour.
    if ((level & 1) || level > 10)
    {
        levelValueMask = (1LL << mNoMiddleLevelBits[level]) - 1;
    }
    else
    {
        levelValueMask = (1LL << mLevelBits[level]) - 1;
    }


    uint64_t levelValue = (toKey >> totalBits) & levelValueMask;
    if (levelValue > mLevelValueLimits[level])
    {
        toKey &= ~(levelValueMask << totalBits);
        toKey |= mLevelValueLimits[level] << totalBits;
    }

    for (; fromKey <= toKey; fromKey += plusUnit)
    {
        //todo: filter useless term
        DateTerm term(fromKey);
        term.SetLevelType(level);
        terms.push_back(term.GetKey());
    }
}

DateTerm DateTerm::ConvertToDateTerm(
        TimestampUtil::Date date,
        DateLevelFormat format)
{
#define FILL_FIELD(field, value)          \
    if (!format.Has##field())             \
    {                                     \
       return tmp;			  \
    }                                     \
    tmp.Set##field(value);

    DateTerm tmp;
    FILL_FIELD(Year, date.year);
    FILL_FIELD(Month,date.month);
    FILL_FIELD(Day, date.day);
    FILL_FIELD(Hour, date.hour);
    FILL_FIELD(Minute, date.minute);
    FILL_FIELD(Second, date.second);
    FILL_FIELD(Millisecond, date.millisecond);
#undef FILL_FIELD
    return tmp;
}

IE_NAMESPACE_END(common);

