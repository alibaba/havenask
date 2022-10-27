#ifndef __INDEXLIB_COUNTER_MAP_H
#define __INDEXLIB_COUNTER_MAP_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/counter/counter_base.h"
#include <autil/Lock.h>

DECLARE_REFERENCE_CLASS(util, Counter);
DECLARE_REFERENCE_CLASS(util, MultiCounter);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(util, StateCounter);

IE_NAMESPACE_BEGIN(util);

class CounterMap
{
public:
    CounterMap();
    ~CounterMap();
public:
    const static std::string EMPTY_COUNTER_MAP_JSON;
public:
    std::vector<CounterPtr> FindCounters(const std::string& nodePath) const;
    std::string Get(const std::string& nodePath) const;

    MultiCounterPtr GetMultiCounter(const std::string& nodePath);
    AccumulativeCounterPtr GetAccCounter(const std::string& nodePath);
    StateCounterPtr GetStateCounter(const std::string& nodePath);

    std::string ToJsonString(bool isCompact = true) const;
    void FromJsonString(const std::string& jsonString);
    void Merge(const std::string& jsonString, CounterBase::FromJsonType fromJsonType);

private:
    CounterBasePtr GetCounter(const std::string& nodePath,
                              CounterBase::CounterType type);
    void CollectSingleCounters(const CounterBasePtr& current,
                               std::vector<CounterPtr>& retCounters) const;
private:
    mutable autil::ThreadMutex mLock;
    MultiCounterPtr mRoot;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CounterMap);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_COUNTER_MAP_H
