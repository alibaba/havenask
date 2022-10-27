#ifndef __INDEXLIB_MEMORY_STAT_REPORTER_H
#define __INDEXLIB_MEMORY_STAT_REPORTER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/monitor.h"

DECLARE_REFERENCE_CLASS(partition, MemoryStatCollector);
DECLARE_REFERENCE_CLASS(util, TaskScheduler);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);
DECLARE_REFERENCE_CLASS(file_system, FileBlockCache);

IE_NAMESPACE_BEGIN(partition);

class MemoryStatReporter
{
public:
    MemoryStatReporter();
    ~MemoryStatReporter();
    
public:
    bool Init(const std::string& param,
              const util::SearchCachePartitionWrapperPtr& searchCache,
              const file_system::FileBlockCachePtr& blockCache,
              const util::TaskSchedulerPtr& taskScheduler,
              misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    
    const MemoryStatCollectorPtr& GetMemoryStatCollector() const
    { return mMemStatCollector; }
    
private:
    int32_t mPrintMetricsTaskId;
    MemoryStatCollectorPtr mMemStatCollector;
    util::TaskSchedulerPtr mTaskScheduler;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MemoryStatReporter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MEMORY_STAT_REPORTER_H
