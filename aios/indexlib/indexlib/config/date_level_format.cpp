#include "indexlib/config/date_level_format.h"

using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, DateLevelFormat);

DateLevelFormat::~DateLevelFormat() 
{
}

void DateLevelFormat::Init(Granularity granularity)
{
    mLevelFormat = DEFALT_LEVEL_FORMAT;
    RemoveLowerGranularity(granularity);
}

void DateLevelFormat::RemoveLowerGranularity(Granularity granularity)
{
    if (granularity == YEAR)
    {
        mLevelFormat &= ~((1<<11) - 1);
        return;
    }

    if (granularity == MONTH)
    {
        mLevelFormat &= ~((1<<10) - 1);
        return;
    }

    if (granularity == DAY)
    {
        mLevelFormat &= ~((1<<8) - 1);
        return;
    }

    if (granularity == HOUR)
    {
        mLevelFormat &= ~((1<<6) - 1);
        return;
    }

    if (granularity == MINUTE)
    {
        mLevelFormat &= ~((1<<4) - 1);
        return;
    }

    if (granularity == SECOND)
    {
        mLevelFormat &= ~((1<<2) - 1);
        return;
    }
}

uint64_t DateLevelFormat::GetGranularityLevel(Granularity granularity)
{
    switch(granularity)
    {
    case MILLISECOND:
        return 1;
    case SECOND:
        return 3;
    case MINUTE:
        return 5;
    case HOUR:
        return 7;
    case DAY:
        return 9;
    case MONTH:
        return 11;
    case YEAR:
        return 12;
    default:
        assert(false);
    }
    return 0;
}

IE_NAMESPACE_END(config);

