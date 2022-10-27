#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "indexlib/misc/exception.h"
#include "indexlib/util/counter/multi_counter.h"
#include "indexlib/util/counter/counter_creator.h"

using namespace std;
using namespace autil::legacy;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, MultiCounter);

MultiCounter::MultiCounter(const string& path)
    : CounterBase(path, CT_DIRECTORY)
{
}

MultiCounter::~MultiCounter() 
{
}

CounterBasePtr MultiCounter::GetCounter(const std::string& name) const
{
    auto iter = mCounterMap.find(name);
    if (iter == mCounterMap.end())
    {
        return CounterBasePtr();
    }
    return iter->second;
}

CounterBasePtr MultiCounter::CreateCounter(const std::string& name, CounterType type)
{
    assert(name.find('.') == string::npos);
    assert(!GetCounter(name));
    assert(type != CT_UNKNOWN);
    auto counter = CounterCreator::CreateCounter(GetFullPath(name), type);
    mCounterMap[name] = counter;
    return counter;
}

autil::legacy::Any MultiCounter::ToJson() const
{
    autil::legacy::json::JsonMap jsonMap;
    jsonMap[TYPE_META] = CounterTypeToStr(mType);
    for (const auto& item : mCounterMap)
    {
        jsonMap[item.first] = item.second->ToJson();
    }
    return jsonMap;
}

void MultiCounter::FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType)
{
    json::JsonMap jsonMap = AnyCast<json::JsonMap>(any);
    for (const auto &item : jsonMap)
    {
        if (item.first == TYPE_META)
        {
            auto counterType = StrToCounterType(AnyCast<string>(item.second));
            if (counterType != CT_DIRECTORY)
            {
                INDEXLIB_FATAL_ERROR(InconsistentState,
                        "counter[%s], type[%u] fromJson failed due to inconsistent type [%u]",
                        mPath.c_str(), CT_DIRECTORY, counterType); 
            }
            continue;
        }
        CounterBasePtr counter;
        try
        {
            json::JsonMap jsonMap = AnyCast<json::JsonMap>(item.second);
            auto iter = jsonMap.find(TYPE_META);
            if (iter == jsonMap.end())
            {
                INDEXLIB_FATAL_ERROR(InconsistentState, "no %s found in counter[%s]",
                        TYPE_META.c_str(), item.first.c_str());
            }
            auto counterType = StrToCounterType(AnyCast<string>(iter->second));
            string counterFullPath = GetFullPath(item.first);
            if (counterType == CT_UNKNOWN)
            {
                INDEXLIB_FATAL_ERROR(InconsistentState, "invalid counter type defined for [%s]",
                        counterFullPath.c_str());
            }
            
            auto counterIter = mCounterMap.find(item.first);
            if (counterIter == mCounterMap.end())
            {
                counter = CounterCreator::CreateCounter(counterFullPath, counterType);
                mCounterMap[item.first] = counter;
            }
            else
            {
                counter = counterIter->second;
                if (counter->GetType() != counterType)
                {
                    INDEXLIB_FATAL_ERROR(InconsistentState,
                            "merge counter[%s], type[%u] failed due to inconsistent type [%u]",
                            mPath.c_str(), counter->GetType(), counterType);
                }
            }
            
            counter->FromJson(item.second, fromJsonType);
        }
        catch(const autil::legacy::BadAnyCast& e)
        {
            INDEXLIB_FATAL_ERROR(InconsistentState,
                    "jsonize for multiCounter[%s] failed!", mPath.c_str());
        }
    }
}

size_t MultiCounter::Sum()
{
    size_t total = 0;
    for (auto iter = mCounterMap.begin(); iter != mCounterMap.end(); ++ iter)
    {
        CounterPtr counter = DYNAMIC_POINTER_CAST(Counter, iter->second);
        if (counter)
        {
            total += counter->Get();
            continue;
        }

        MultiCounterPtr multiCounter = DYNAMIC_POINTER_CAST(MultiCounter, iter->second);
        if (multiCounter)
        {
            total += multiCounter->Sum();
        }
    }
    return total;
}

IE_NAMESPACE_END(util);

