#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/config/online_config.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(config);
IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OnlinePartitionMetrics);

OnlinePartitionMetrics::OnlinePartitionMetrics(MetricProviderPtr metricProvider)
    : mMetricProvider(metricProvider)
    , mAttributeMetrics(metricProvider)
    , mIndexMetrics(metricProvider)
    , mPrintMetricsTsInSeconds(0)
{
    mPrintMetricsTsInSeconds = TimeUtility::currentTimeInSeconds();
}

OnlinePartitionMetrics::~OnlinePartitionMetrics() 
{
}

void OnlinePartitionMetrics::RegisterOnlineMetrics()
{
#define INIT_ONLINE_PARTITION_METRIC(metric, unit)                      \
    m##metric = 0L;                                                     \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "online/"#metric, kmonitor::GAUGE, unit)

    IE_INIT_METRIC_GROUP(mMetricProvider,
                         rtIndexMemoryUseRatio,
                         "online/rtIndexMemoryUseRatio",
                         kmonitor::STATUS, "%");
    INIT_ONLINE_PARTITION_METRIC(partitionIndexSize, "byte");
    INIT_ONLINE_PARTITION_METRIC(partitionMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(incIndexMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(rtIndexMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(builtRtIndexMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(buildingSegmentMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(oldInMemorySegmentMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(partitionMemoryQuotaUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(memoryStatus, "count");
#undef INIT_ONLINE_PARTITION_METRIC

    IE_INIT_METRIC_GROUP(mMetricProvider, dumpingSegmentCount,
                         "online/dumpingSegmentCount", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, indexPhase,
                         "online/indexPhase", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, partitionReaderVersionCount,
                         "online/partitionReaderVersionCount", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, latestReaderVersionId,
                         "online/latestReaderVersionId", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, oldestReaderVersionId,
                         "online/oldestReaderVersionId", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, missingSegmentCount,
                         "online/missingSegmentCount", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, missingSegment,
                         "online/missingSegment", kmonitor::STATUS, "count");
}

void OnlinePartitionMetrics::RegisterReOpenMetrics(TableType tableType)
{
#define INIT_REOPEN_METRIC(metric)                                      \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "reopen/"#metric, kmonitor::GAUGE, "ms")
    
    INIT_REOPEN_METRIC(reopenIncLatency);
    INIT_REOPEN_METRIC(reopenRealtimeLatency);
    
    INIT_REOPEN_METRIC(ReclaimRtIndexLatency);
    INIT_REOPEN_METRIC(ReopenFullPartitionLatency);
    
    INIT_REOPEN_METRIC(decisionLatency);
    INIT_REOPEN_METRIC(preloadLatency);
    INIT_REOPEN_METRIC(SwitchFlushRtSegmentLatency);
    
    if (tableType != tt_kv && tableType != tt_kkv && tableType != tt_customized)
    {
        INIT_REOPEN_METRIC(GenerateJoinSegmentLatency);
        INIT_REOPEN_METRIC(createJoinSegmentWriterLatency);
        INIT_REOPEN_METRIC(prejoinLatency);
        INIT_REOPEN_METRIC(LoadReaderPatchLatency);
        INIT_REOPEN_METRIC(ReclaimReaderMemLatency);
    }

#undef INIT_REOPEN_METRIC
}

void OnlinePartitionMetrics::RegisterBuildMetrics()
{
    IE_INIT_METRIC_GROUP(mMetricProvider, obsoleteDocQps,
                         "build/obsoleteDocQps", kmonitor::QPS, "count");
}

void OnlinePartitionMetrics::RegisterMetrics(TableType tableType)
{
    RegisterOnlineMetrics();
    RegisterReOpenMetrics(tableType);
    RegisterBuildMetrics();
    mAttributeMetrics.RegisterMetrics(tableType);
}

void OnlinePartitionMetrics::ReportOnlineMetrics()
{
    IE_REPORT_METRIC(memoryStatus, mmemoryStatus);
    IE_REPORT_METRIC(indexPhase, mindexPhase);
    IE_REPORT_METRIC(dumpingSegmentCount, mdumpingSegmentCount);
    IE_REPORT_METRIC(partitionIndexSize, mpartitionIndexSize);
    IE_REPORT_METRIC(partitionMemoryUse, mpartitionMemoryUse);
    IE_REPORT_METRIC(incIndexMemoryUse, mincIndexMemoryUse);
    IE_REPORT_METRIC(rtIndexMemoryUse, mrtIndexMemoryUse);
    IE_REPORT_METRIC(rtIndexMemoryUseRatio, mrtIndexMemoryUseRatio);
    IE_REPORT_METRIC(builtRtIndexMemoryUse, mbuiltRtIndexMemoryUse);
    IE_REPORT_METRIC(buildingSegmentMemoryUse, mbuildingSegmentMemoryUse);
    IE_REPORT_METRIC(oldInMemorySegmentMemoryUse, moldInMemorySegmentMemoryUse);
    IE_REPORT_METRIC(partitionMemoryQuotaUse, mpartitionMemoryQuotaUse);
    IE_REPORT_METRIC(partitionReaderVersionCount, mpartitionReaderVersionCount);
    IE_REPORT_METRIC(latestReaderVersionId, mlatestReaderVersionId);
    IE_REPORT_METRIC(oldestReaderVersionId, moldestReaderVersionId);
    IE_REPORT_METRIC(missingSegmentCount, mmissingSegmentCount);
    IE_REPORT_METRIC(missingSegment, mmissingSegmentCount > 0 ? 1 : 0);
}

void OnlinePartitionMetrics::ReportMetrics()
{
    ReportOnlineMetrics();
    mAttributeMetrics.ReportMetrics();
}

void OnlinePartitionMetrics::IncreateObsoleteDocQps()
{
    IE_INCREASE_QPS(obsoleteDocQps);
}

void OnlinePartitionMetrics::PrintMetrics(
        const OnlineConfig& onlineConfig, const string& partitionName)
{
    int64_t currentTsInSeconds = TimeUtility::currentTimeInSeconds();
    if (currentTsInSeconds - mPrintMetricsTsInSeconds > onlineConfig.printMetricsInterval)
    {
        IE_LOG(INFO, "Print OnlinePartitionMetrics for table [%s], "
               "partitionIndexSize [%ld], partitionMemoryUse [%ld], incIndexMemoryUse [%ld], "
               "rtIndexMemoryUse [%ld], builtRtIndexMemoryUse [%ld], buildingSegmentMemoryUse [%ld], "
               "oldInMemorySegmentMemoryUse [%ld], partitionMemoryQuotaUse [%ld], "
               "partitionReaderVersionCount [%ld], "
               "latestReaderVersionId [%d], oldestReaderVersionId [%d], missingSegmentCount [%ld]",
               partitionName.c_str(), mpartitionIndexSize, mpartitionMemoryUse,
               mincIndexMemoryUse, mrtIndexMemoryUse, mbuiltRtIndexMemoryUse,
               mbuildingSegmentMemoryUse, moldInMemorySegmentMemoryUse, mpartitionMemoryQuotaUse,
               mpartitionReaderVersionCount, mlatestReaderVersionId, moldestReaderVersionId,
               mmissingSegmentCount);

        mPrintMetricsTsInSeconds = currentTsInSeconds;
    }
}

IE_NAMESPACE_END(partition);

