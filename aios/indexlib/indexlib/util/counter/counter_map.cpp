#include <autil/StringUtil.h>
#include <autil/legacy/jsonizable.h>
#include "indexlib/util/counter/counter_map.h"
#include "indexlib/util/counter/counter.h"
#include "indexlib/util/counter/multi_counter.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/counter/state_counter.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, CounterMap);

CounterMap::CounterMap()
    : mRoot(new MultiCounter(""))
{
}

CounterMap::~CounterMap() 
{
}

const string CounterMap::EMPTY_COUNTER_MAP_JSON = "{}";

std::string CounterMap::Get(const std::string& nodePath) const
{
    ScopedLock l(mLock);
    vector<string> pathVec = StringUtil::split(nodePath, ".", false);
    CounterBasePtr current = mRoot;
    const string INVALID_RESULT = "";
    for (const auto& name : pathVec)
    {
        MultiCounterPtr multiCounter = DYNAMIC_POINTER_CAST(MultiCounter, current);
        if (!multiCounter)
        {
            return INVALID_RESULT;
        }
        current = multiCounter->GetCounter(name);
    }
    if (!current)
    {
        return INVALID_RESULT;
    }
    return autil::legacy::json::ToString(current->ToJson());
}

CounterBasePtr CounterMap::GetCounter(const string& nodePath, CounterBase::CounterType type)
{
    ScopedLock l(mLock);
    vector<string> pathVec = StringUtil::split(nodePath, ".", false);
    CounterBasePtr current = mRoot;
    for (size_t i = 0; i < pathVec.size(); ++i)
    {
        const auto& name = pathVec[i];
        MultiCounterPtr multiCounter = DYNAMIC_POINTER_CAST(MultiCounter, current);
        if (!multiCounter)
        {
            return CounterPtr();
        }
        current = multiCounter->GetCounter(name);
        if (i == pathVec.size() - 1)
        {
            if (!current)
            {
                current = multiCounter->CreateCounter(name, type);
            }
            return current;
        }
        else
        {
            if (!current)
            {
                current = multiCounter->CreateCounter(name, CounterBase::CT_DIRECTORY);
            }
        }
    }
    return CounterPtr();
}

MultiCounterPtr CounterMap::GetMultiCounter(const std::string& nodePath)
{
    return DYNAMIC_POINTER_CAST(
            MultiCounter, GetCounter(nodePath, CounterBase::CT_DIRECTORY));
}

AccumulativeCounterPtr CounterMap::GetAccCounter(const std::string& nodePath)
{
    return DYNAMIC_POINTER_CAST(
            AccumulativeCounter, GetCounter(nodePath, CounterBase::CT_ACCUMULATIVE));
}

StateCounterPtr CounterMap::GetStateCounter(const std::string& nodePath)
{
    return DYNAMIC_POINTER_CAST(
            StateCounter, GetCounter(nodePath, CounterBase::CT_STATE));
}

vector<CounterPtr> CounterMap::FindCounters(const std::string& nodePath) const
{
    ScopedLock l(mLock);
    vector<string> pathVec = StringUtil::split(nodePath, ".", true);
    vector<CounterPtr> retCounters;
    MultiCounterPtr parent = mRoot;
    CounterBasePtr current = parent;
    
    for (size_t i = 0; parent && i < pathVec.size(); ++i)
    {
        current = parent->GetCounter(pathVec[i]);
        if (!current)
        {
            break;
        }

        if (i < pathVec.size() - 1)
        {
            parent = DYNAMIC_POINTER_CAST(MultiCounter, current);
            current.reset();
        }
    }

    if (!current)
    {
        return retCounters;
    }
    
    CollectSingleCounters(current, retCounters);
    return retCounters;
}

void CounterMap::CollectSingleCounters(const CounterBasePtr& current, vector<CounterPtr>& retCounters) const
{
    if (!current)
    {
        return;
    }

    CounterPtr singleCounter = DYNAMIC_POINTER_CAST(Counter, current);
    if (singleCounter)
    {
        retCounters.push_back(singleCounter);
        return;
    }

    MultiCounterPtr multiCounter = DYNAMIC_POINTER_CAST(MultiCounter, current);
    assert(multiCounter);

    const auto& subCounterMap = multiCounter->GetCounterMap();

    for (auto& kv : subCounterMap)
    {
        CollectSingleCounters(kv.second, retCounters);
    }
}

std::string CounterMap::ToJsonString(bool isCompact) const
{
    ScopedLock l(mLock);
    return autil::legacy::json::ToString(mRoot->ToJson(), isCompact);
}

void CounterMap::FromJsonString(const std::string& jsonString)
{
    ScopedLock l(mLock);
    mRoot.reset(new MultiCounter(""));
    json::JsonMap jsonMap;
    autil::legacy::FromJsonString(jsonMap, jsonString);
    mRoot->FromJson(jsonMap, CounterBase::FJT_OVERWRITE);
}

void CounterMap::Merge(const std::string& jsonString, CounterBase::FromJsonType fromJsonType)
{
    ScopedLock l(mLock);
    if (!mRoot)
    {
        mRoot.reset(new MultiCounter(""));
    }
    json::JsonMap jsonMap;
    autil::legacy::FromJsonString(jsonMap, jsonString);        
    mRoot->FromJson(jsonMap, fromJsonType);
}

IE_NAMESPACE_END(util);

