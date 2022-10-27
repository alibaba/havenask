#ifndef __INDEXLIB_DATE_LEVEL_FORMAT_H
#define __INDEXLIB_DATE_LEVEL_FORMAT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(config);

class DateLevelFormat
{
private:
    static const uint16_t DEFALT_LEVEL_FORMAT = 4095;
public:
    DateLevelFormat()
        : mLevelFormat(DEFALT_LEVEL_FORMAT)
    {}
    ~DateLevelFormat();
    
public:
    enum Granularity
    {
        GU_UNKOWN,
        MILLISECOND,
        SECOND,
        MINUTE,
        HOUR,
        DAY,
        MONTH,
        YEAR
    };
    void Init(Granularity buildGranularity);
    uint16_t GetLevelFormatNumber() const { return mLevelFormat; }
    bool HasLevel(uint64_t level) const { return mLevelFormat >> (level - 1) & 1; }
    bool HasMillisecond() const { return mLevelFormat & 1; }
    bool HasMiddleSecond() const { return mLevelFormat >> 1 & 1; }
    bool HasSecond() const { return mLevelFormat >> 2 & 1; }
    bool HasMiddleMinute() const { return mLevelFormat >> 3 & 1; }
    bool HasMinute() const { return mLevelFormat >> 4 & 1; }
    bool HasMiddleHour() const { return mLevelFormat >> 5 & 1; }
    bool HasHour() const { return mLevelFormat >> 6 & 1; }
    bool HasMiddleDay() const { return mLevelFormat >> 7 & 1; }
    bool HasDay() const { return mLevelFormat >> 8 & 1; }
    bool HasMiddleMonth() const { return mLevelFormat >> 9 & 1; }
    bool HasMonth() const { return mLevelFormat >> 10 & 1; }
    bool HasYear() const { return mLevelFormat >> 11 & 1; }

    static uint64_t GetGranularityLevel(Granularity granularity);
    bool operator==(const DateLevelFormat& other) const
    {
        return mLevelFormat == other.mLevelFormat;
    }
    
public:
    //only for test
    void Init(uint16_t levelFormat, Granularity buildGranularity)
    {
        mLevelFormat = levelFormat;
        RemoveLowerGranularity(buildGranularity);
    }

private:
    void RemoveLowerGranularity(Granularity granularity);
private:
    uint16_t mLevelFormat;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DateLevelFormat);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_DATE_LEVEL_FORMAT_H
