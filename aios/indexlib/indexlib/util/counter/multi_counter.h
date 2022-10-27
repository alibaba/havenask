#ifndef __INDEXLIB_MULTI_COUNTER_H
#define __INDEXLIB_MULTI_COUNTER_H

#include <tr1/memory>
#include <map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/counter/counter_base.h"
#include "indexlib/util/counter/counter.h"
#include "indexlib/util/counter/accumulative_counter.h"
#include "indexlib/util/counter/state_counter.h"

IE_NAMESPACE_BEGIN(util);

class MultiCounter;
DEFINE_SHARED_PTR(MultiCounter);

class MultiCounter : public CounterBase
{
public:
    MultiCounter(const std::string& path);
    ~MultiCounter();
public:
    CounterBasePtr GetCounter(const std::string& name) const;
    CounterBasePtr CreateCounter(const std::string& name, CounterType type);

    const std::map<std::string, CounterBasePtr>& GetCounterMap() const
    { return mCounterMap; }
    
public:
    inline MultiCounterPtr CreateMultiCounter(const std::string& name) {
        return DYNAMIC_POINTER_CAST(
            MultiCounter, CreateCounter(name, CounterBase::CT_DIRECTORY));
    }
    inline AccumulativeCounterPtr CreateAccCounter(const std::string& name) {
        return DYNAMIC_POINTER_CAST(
            AccumulativeCounter, CreateCounter(name, CounterBase::CT_ACCUMULATIVE));
    }
    inline StateCounterPtr CreateStateCounter(const std::string& name) {
        return DYNAMIC_POINTER_CAST(
            StateCounter, CreateCounter(name, CounterBase::CT_STATE));
    }
    size_t Sum();

public:
    autil::legacy::Any ToJson() const override;
    void FromJson(const autil::legacy::Any& any, FromJsonType fromJsonType) override;

private:
    std::string GetFullPath(const std::string& name) const
    {
        return mPath.empty() ? name : mPath + "." + name;
    }
        
private:
    std::map<std::string, CounterBasePtr> mCounterMap;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MULTI_COUNTER_H
