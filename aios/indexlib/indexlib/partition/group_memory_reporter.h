#ifndef __INDEXLIB_GROUP_MEMORY_REPORTER_H
#define __INDEXLIB_GROUP_MEMORY_REPORTER_H

#include <tr1/memory>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/monitor.h"

IE_NAMESPACE_BEGIN(partition);

class GroupMemoryReporter
{
public:
    GroupMemoryReporter(misc::MetricProviderPtr metricProvider,
                        const std::string& groupName);
    ~GroupMemoryReporter();
    
public:
    void Reset();
    void ReportMetrics();

private:
    IE_DECLARE_PARAM_METRIC(int64_t, totalPartitionIndexSize);
    // memory use
    IE_DECLARE_PARAM_METRIC(int64_t, totalPartitionMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, totalIncIndexMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, totalRtIndexMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, totalBuiltRtIndexMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, totalBuildingSegmentMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, totalOldInMemorySegmentMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, totalPartitionMemoryQuotaUse);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(GroupMemoryReporter);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_GROUP_MEMORY_REPORTER_H
