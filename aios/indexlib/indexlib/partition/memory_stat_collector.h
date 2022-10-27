#ifndef __INDEXLIB_MEMORY_STAT_COLLECTOR_H
#define __INDEXLIB_MEMORY_STAT_COLLECTOR_H

#include <tr1/memory>
#include <unordered_map>
#include "indexlib/misc/metric_provider.h"
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/misc/monitor.h"

DECLARE_REFERENCE_CLASS(partition, GroupMemoryReporter);
DECLARE_REFERENCE_CLASS(partition, OnlinePartitionMetrics);
DECLARE_REFERENCE_CLASS(util, SearchCachePartitionWrapper);
DECLARE_REFERENCE_CLASS(file_system, FileBlockCache);

IE_NAMESPACE_BEGIN(partition);

class MemoryStatCollector
{
public:
    MemoryStatCollector(const util::SearchCachePartitionWrapperPtr& searchCache,
                        const file_system::FileBlockCachePtr& blockCache,
                        misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    
    ~MemoryStatCollector() {}
    
public:
    void AddTableMetrics(const std::string& partitionGroupName,
                         const partition::OnlinePartitionMetricsPtr& tableMetrics);
    void PrintMetrics();
    void ReportMetrics();

public:
    // for test
    GroupMemoryReporterPtr GetGroupMemoryReporter(const std::string& groupName);
    
private:
    void InnerInitMetrics();
    void InnerUpdateMetrics();
    void UpdateGroupMetrics(const std::string& groupName,
                            const partition::OnlinePartitionMetricsPtr& partMetrics);
    
private:
    typedef std::pair<std::string, partition::OnlinePartitionMetricsPtr> MetricsItem;
    typedef std::unordered_map<std::string, GroupMemoryReporterPtr> GroupReporterMap;
    
    autil::RecursiveThreadMutex mLock;
    std::vector<MetricsItem> mMetricsVec;
    util::SearchCachePartitionWrapperPtr mSearchCache;
    file_system::FileBlockCachePtr mBlockCache;
    misc::MetricProviderPtr mMetricProvider;
    GroupReporterMap mGroupReporterMap;
    bool mMetricsInitialized;

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

DEFINE_SHARED_PTR(MemoryStatCollector);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MEMORY_STAT_COLLECTOR_H
