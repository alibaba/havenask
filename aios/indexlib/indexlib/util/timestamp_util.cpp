#include "indexlib/util/timestamp_util.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, TimestampUtil);

int TimestampUtil::mDays[12] = {31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
int TimestampUtil::mLpDays[12] = {31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};
int TimestampUtil::mDayToMonth[366] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,0};
int TimestampUtil::mLpDayToMonth[366] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,4,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,5,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,6,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,10,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11};

TimestampUtil::TimestampUtil()
{
}

TimestampUtil::~TimestampUtil()
{
}

TimestampUtil::Date TimestampUtil::ConvertToDate(uint64_t timp)
{
    if (unlikely(timp > MAX_TIME_SUPPORT)) {
        timp = MAX_TIME_SUPPORT;
    }
    uint64_t caltim = timp / 1000;
    int islpyr = 0;
    Date result;
    result.millisecond = timp % 1000;
    int32_t tmptim = (int32_t)(caltim / _FOUR_YEAR_SEC);
    caltim -= ((uint64_t)tmptim * _FOUR_YEAR_SEC);
    int* mdays = NULL;
    int *mydays = NULL;
    tmptim *= 4;
    if (caltim >= _YEAR_SEC) {
        ++tmptim;
        caltim -= _YEAR_SEC;
        if (caltim >= _YEAR_SEC) {
            ++tmptim;
            caltim -= _YEAR_SEC;
            if (caltim >= (_YEAR_SEC + _DAY_SEC)) {
                ++tmptim;
                caltim -= (_YEAR_SEC + _DAY_SEC);
            } else {
                ++islpyr;
            }
        }
    }
    result.year = tmptim;
    int32_t tm_yday = (int32_t)(caltim / _DAY_SEC);
    caltim -= (uint64_t)(tm_yday) * _DAY_SEC;

    if (islpyr) {
        mdays = mLpDays;
        mydays = mLpDayToMonth;
    } else {
        mdays = mDays;
        mydays = mDayToMonth;
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


IE_NAMESPACE_END(util);
