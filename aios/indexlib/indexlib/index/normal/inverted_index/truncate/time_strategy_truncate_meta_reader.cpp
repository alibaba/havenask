#include "indexlib/index/normal/inverted_index/truncate/time_strategy_truncate_meta_reader.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TimeStrategyTruncateMetaReader);

TimeStrategyTruncateMetaReader::TimeStrategyTruncateMetaReader(
        int64_t minTime, int64_t maxTime, bool desc)
    : TruncateMetaReader(desc)
    , mMinTime(minTime)
    , mMaxTime(maxTime)
{
}

TimeStrategyTruncateMetaReader::~TimeStrategyTruncateMetaReader() 
{
}

bool TimeStrategyTruncateMetaReader::Lookup(dictkey_t key, int64_t &min, int64_t &max)
{
    DictType::iterator it = mDict.find(key);
    if (it == mDict.end())
    {
        return false;
    }

    if (mDesc)
    {
        min = std::min(mMinTime, it->second.first);
        max = mMaxTime;
    }
    else
    {
        min = mMinTime;
        max = std::max(mMaxTime, it->second.second);
    }
    return true;
}

IE_NAMESPACE_END(index);

