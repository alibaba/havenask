#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "indexlib/util/counter/counter.h"
#include "indexlib/misc/exception.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);

IE_LOG_SETUP(util, Counter);

Counter::Counter(const string& path, CounterType type)
    : CounterBase(path, type)
    , mMergedSum(0)
{
}

Counter::~Counter() 
{
}

Any Counter::ToJson() const
{
    autil::legacy::json::JsonMap jsonMap;
    jsonMap[TYPE_META] = CounterTypeToStr(mType);
    jsonMap["value"] = Get();
    return jsonMap;
}

void Counter::FromJson(const Any& any, FromJsonType fromJsonType)
{
    json::JsonMap jsonMap = AnyCast<json::JsonMap>(any);
    auto iter = jsonMap.find("value");
    if (iter == jsonMap.end())
    {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "no value found in counter[%s]", mPath.c_str());        
    }
    
    mMergedSum = AnyCast<json::JsonNumber>(iter->second).Cast<int64_t>();
}

IE_NAMESPACE_END(util);

