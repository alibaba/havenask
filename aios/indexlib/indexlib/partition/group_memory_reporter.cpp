#include "indexlib/partition/group_memory_reporter.h"

using namespace std;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, GroupMemoryReporter);

GroupMemoryReporter::GroupMemoryReporter(
        MetricProviderPtr metricProvider, const string& groupName)
{
    assert(metricProvider);
    
#define INIT_MEM_STAT_METRIC(groupName, metric, unit)                   \
    {                                                                   \
        m##metric = 0L;                                                 \
        string metricName = string("group/") + groupName + "/"#metric;  \
        IE_INIT_METRIC_GROUP(metricProvider, metric, metricName, kmonitor::GAUGE, unit); \
    }                                                                   \

    INIT_MEM_STAT_METRIC(groupName, totalPartitionIndexSize, "byte");
    INIT_MEM_STAT_METRIC(groupName, totalPartitionMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(groupName, totalIncIndexMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(groupName, totalRtIndexMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(groupName, totalBuiltRtIndexMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(groupName, totalBuildingSegmentMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(groupName, totalOldInMemorySegmentMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(groupName, totalPartitionMemoryQuotaUse, "byte");

#undef INIT_MEM_STAT_METRIC
}

GroupMemoryReporter::~GroupMemoryReporter() 
{
}

void GroupMemoryReporter::Reset()
{
    mtotalPartitionIndexSize = 0;
    mtotalPartitionMemoryUse = 0;
    mtotalIncIndexMemoryUse = 0;
    mtotalRtIndexMemoryUse = 0;
    mtotalBuiltRtIndexMemoryUse = 0;
    mtotalBuildingSegmentMemoryUse = 0;
    mtotalOldInMemorySegmentMemoryUse = 0;
    mtotalPartitionMemoryQuotaUse = 0;
}

void GroupMemoryReporter::ReportMetrics()
{
    IE_REPORT_METRIC(totalPartitionIndexSize, mtotalPartitionIndexSize);
    IE_REPORT_METRIC(totalPartitionMemoryUse, mtotalPartitionMemoryUse);
    IE_REPORT_METRIC(totalIncIndexMemoryUse, mtotalIncIndexMemoryUse);
    IE_REPORT_METRIC(totalRtIndexMemoryUse, mtotalRtIndexMemoryUse);
    IE_REPORT_METRIC(totalBuiltRtIndexMemoryUse, mtotalBuiltRtIndexMemoryUse);
    IE_REPORT_METRIC(totalBuildingSegmentMemoryUse, mtotalBuildingSegmentMemoryUse);
    IE_REPORT_METRIC(totalOldInMemorySegmentMemoryUse, mtotalOldInMemorySegmentMemoryUse);
    IE_REPORT_METRIC(totalPartitionMemoryQuotaUse, mtotalPartitionMemoryQuotaUse);
}

IE_NAMESPACE_END(partition);

