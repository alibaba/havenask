#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(util);

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, AccumulativeCounter);

AccumulativeCounter::AccumulativeCounter(const string& path)
    : Counter(path, CT_ACCUMULATIVE)
    , mThreadValue(new ThreadLocalPtr(&MergeThreadValue))
{
}

AccumulativeCounter::~AccumulativeCounter() 
{
}

void AccumulativeCounter::FromJson(const Any& any, FromJsonType fromJsonType)
{
    json::JsonMap jsonMap = AnyCast<json::JsonMap>(any);
    auto iter = jsonMap.find("value");
    if (iter == jsonMap.end())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "no value found in counter[%s]", mPath.c_str()); 
    }

    int64_t value = AnyCast<json::JsonNumber>(iter->second).Cast<int64_t>();
    if (fromJsonType == FJT_OVERWRITE)
    {
        mMergedSum = value;
    }
    else if (fromJsonType == FJT_MERGE)
    {
        Increase(value);
    }
    else
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "invalid fromJsonType[%u] for counter[%s]",
                             fromJsonType, mPath.c_str()); 
    }
}

IE_NAMESPACE_END(util);

