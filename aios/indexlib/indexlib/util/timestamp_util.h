#ifndef __INDEXLIB_TIMESTAMP_UTIL_H
#define __INDEXLIB_TIMESTAMP_UTIL_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(util);

class TimestampUtil
{
public:
    struct Date
    {
        Date()
            : year(0)
            , month(0)
            , day(0)
            , hour(0)
            , minute(0)
            , second(0)
            , millisecond(0)
        {}

        uint64_t year:9;
        uint64_t month:4;
        uint64_t day:5;
        uint64_t hour:5;
        uint64_t minute:6;
        uint64_t second:6;
        uint64_t millisecond:10;
    };
public:
    TimestampUtil();
    ~TimestampUtil();
public:
    static Date ConvertToDate(uint64_t miliseconds);

private:
    static const int64_t _FOUR_YEAR_SEC=126230400;
    static const int64_t _YEAR_SEC=31536000;
    static const int64_t _DAY_SEC=86400;
    static const int64_t MAX_TIME_SUPPORT = 4102444800000;//2100-01-01 00:00:00
    static int mDays[12];
    static int mLpDays[12];
    static int mDayToMonth[366];
    static int mLpDayToMonth[366];
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TimestampUtil);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TIMESTAMP_UTIL_H
