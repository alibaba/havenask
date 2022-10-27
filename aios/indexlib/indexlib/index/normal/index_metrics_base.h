#ifndef __INDEXLIB_INDEX_METRICS_BASE_H
#define __INDEXLIB_INDEX_METRICS_BASE_H

#include <tr1/memory>
#include <autil/AtomicCounter.h>
#include "indexlib/misc/metric_provider.h"
#include <unordered_map>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/counter/accumulative_counter.h"

IE_NAMESPACE_BEGIN(index);

class IndexMetricsBase
{
public:
    typedef std::unordered_map<std::string, util::AccumulativeCounterPtr> AccessCounterMap;

public:
    IndexMetricsBase() {}
    virtual ~IndexMetricsBase() {}

public:
    const AccessCounterMap& GetAccessCounters() const { return mAccessCounters; }
    void IncAccessCounter(const std::string& indexName)
    {
        AccessCounterMap::iterator iter = mAccessCounters.find(indexName);
        if (iter != mAccessCounters.end() && iter->second.get() != nullptr)
        {
            iter->second->Increase(1);
        }
    }
    void AddAccessCounter(const std::string& indexName, const util::AccumulativeCounterPtr& counter)
    {
        if (mAccessCounters.count(indexName) == 0)
        {
            mAccessCounters.insert({indexName, counter});
        }        
    }

private:
    AccessCounterMap mAccessCounters;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexMetricsBase);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_INDEX_METRICS_BASE_H
