#ifndef __INDEXLIB_ONLINE_PARTITION_METRICS_H
#define __INDEXLIB_ONLINE_PARTITION_METRICS_H

#include <tr1/memory>
#include <autil/TimeUtility.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index/normal/inverted_index/index_metrics.h"

DECLARE_REFERENCE_CLASS(config, OnlineConfig);

IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionMetrics
{
public:
    OnlinePartitionMetrics(
            misc::MetricProviderPtr metricProvider = misc::MetricProviderPtr());
    ~OnlinePartitionMetrics();

public:
    void RegisterMetrics(TableType tableType);
    void ReportMetrics();
    void IncreateObsoleteDocQps();

    index::AttributeMetrics* GetAttributeMetrics()
    { return &mAttributeMetrics; }
    index::IndexMetrics* GetIndexMetrics()
    { return &mIndexMetrics; }
    misc::MetricProviderPtr GetMetricProvider()
    { return mMetricProvider; }

    void PrintMetrics(const config::OnlineConfig& onlineConfig,
                      const std::string& partitionName);

private:
    void RegisterOnlineMetrics();
    void RegisterReOpenMetrics(TableType tableType);
    void RegisterBuildMetrics();

    void ReportOnlineMetrics();

private:
    misc::MetricProviderPtr mMetricProvider;
    index::AttributeMetrics mAttributeMetrics;
    index::IndexMetrics mIndexMetrics;

    int64_t mPrintMetricsTsInSeconds;

    IE_DECLARE_PARAM_METRIC(int64_t, memoryStatus);
    IE_DECLARE_PARAM_METRIC(int64_t, indexPhase);
    IE_DECLARE_PARAM_METRIC(int64_t, dumpingSegmentCount);
    IE_DECLARE_PARAM_METRIC(int64_t, partitionIndexSize);
    IE_DECLARE_PARAM_METRIC(int64_t, partitionReaderVersionCount);
    IE_DECLARE_PARAM_METRIC(int32_t, latestReaderVersionId);
    IE_DECLARE_PARAM_METRIC(int32_t, oldestReaderVersionId);
    IE_DECLARE_PARAM_METRIC(int64_t, missingSegmentCount);
    IE_DECLARE_PARAM_METRIC(int64_t, missingSegment);

    // memory use
    IE_DECLARE_PARAM_METRIC(int64_t, partitionMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, incIndexMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, rtIndexMemoryUse);
    IE_DECLARE_PARAM_METRIC(double, rtIndexMemoryUseRatio);
    IE_DECLARE_PARAM_METRIC(int64_t, builtRtIndexMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, buildingSegmentMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, oldInMemorySegmentMemoryUse);
    IE_DECLARE_PARAM_METRIC(int64_t, partitionMemoryQuotaUse);

    // reopen
    IE_DECLARE_REACHABLE_METRIC(reopenIncLatency);
    IE_DECLARE_REACHABLE_METRIC(reopenRealtimeLatency);
    IE_DECLARE_REACHABLE_METRIC(ReclaimRtIndexLatency);
    IE_DECLARE_REACHABLE_METRIC(GenerateJoinSegmentLatency);
    IE_DECLARE_REACHABLE_METRIC(ReopenFullPartitionLatency);
    IE_DECLARE_REACHABLE_METRIC(LoadReaderPatchLatency);
    IE_DECLARE_REACHABLE_METRIC(decisionLatency);
    IE_DECLARE_REACHABLE_METRIC(createJoinSegmentWriterLatency);
    IE_DECLARE_REACHABLE_METRIC(preloadLatency);
    IE_DECLARE_REACHABLE_METRIC(prejoinLatency);
    IE_DECLARE_REACHABLE_METRIC(ReclaimReaderMemLatency);
    IE_DECLARE_REACHABLE_METRIC(SwitchFlushRtSegmentLatency);

    // build
    IE_DECLARE_REACHABLE_METRIC(obsoleteDocQps);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlinePartitionMetrics);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINE_PARTITION_METRICS_H
