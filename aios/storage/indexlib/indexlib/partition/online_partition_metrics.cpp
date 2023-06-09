/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "indexlib/partition/online_partition_metrics.h"

#include "indexlib/config/online_config.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;
using namespace indexlib::config;
namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, OnlinePartitionMetrics);

OnlinePartitionMetrics::OnlinePartitionMetrics(MetricProviderPtr metricProvider)
    : mMetricProvider(metricProvider)
    , mAttributeMetrics(metricProvider)
    , mIndexMetrics(metricProvider)
    , mPrintMetricsTsInSeconds(0)
{
    mPrintMetricsTsInSeconds = TimeUtility::currentTimeInSeconds();
}

OnlinePartitionMetrics::~OnlinePartitionMetrics() {}

void OnlinePartitionMetrics::RegisterOnlineMetrics()
{
#define INIT_ONLINE_PARTITION_METRIC(metric, unit)                                                                     \
    m##metric = 0L;                                                                                                    \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "online/" #metric, kmonitor::GAUGE, unit)

    IE_INIT_METRIC_GROUP(mMetricProvider, rtIndexMemoryUseRatio, "online/rtIndexMemoryUseRatio", kmonitor::STATUS, "%");

    IE_INIT_METRIC_GROUP(mMetricProvider, partitionMemoryQuotaUseRatio, "online/partitionMemoryQuotaUseRatio",
                         kmonitor::STATUS, "%");

    IE_INIT_METRIC_GROUP(mMetricProvider, freeMemoryQuotaRatio, "online/freeMemoryQuotaRatio", kmonitor::STATUS, "%");

    INIT_ONLINE_PARTITION_METRIC(partitionIndexSize, "byte");
    INIT_ONLINE_PARTITION_METRIC(partitionMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(incIndexMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(rtIndexMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(builtRtIndexMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(buildingSegmentMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(oldInMemorySegmentMemoryUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(partitionMemoryQuotaUse, "byte");
    INIT_ONLINE_PARTITION_METRIC(totalMemoryQuotaLimit, "byte");
    INIT_ONLINE_PARTITION_METRIC(freeMemoryQuota, "byte");

    INIT_ONLINE_PARTITION_METRIC(memoryStatus, "count");
#undef INIT_ONLINE_PARTITION_METRIC

    IE_INIT_METRIC_GROUP(mMetricProvider, dumpingSegmentCount, "online/dumpingSegmentCount", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, indexPhase, "online/indexPhase", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, partitionReaderVersionCount, "online/partitionReaderVersionCount",
                         kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, latestReaderVersionId, "online/latestReaderVersionId", kmonitor::STATUS,
                         "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, oldestReaderVersionId, "online/oldestReaderVersionId", kmonitor::STATUS,
                         "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, missingSegmentCount, "online/missingSegmentCount", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, missingSegment, "online/missingSegment", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, inMemRtSegmentCount, "online/inMemRtSegmentCount", kmonitor::STATUS, "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, onDiskRtSegmentCount, "online/onDiskRtSegmentCount", kmonitor::STATUS,
                         "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, usedOnDiskRtSegmentCount, "online/usedOnDiskRtSegmentCount", kmonitor::STATUS,
                         "count");
    IE_INIT_METRIC_GROUP(mMetricProvider, RedoOperationLogTimeused, "online/RedoOperationLogTimeused", kmonitor::GAUGE,
                         "ms");
    IE_INIT_METRIC_GROUP(mMetricProvider, RedoOperationLockRealtimeTimeused, "online/RedoOperationLockRealtimeTimeused",
                         kmonitor::STATUS, "ms");
}

void OnlinePartitionMetrics::RegisterReOpenMetrics(TableType tableType)
{
#define INIT_REOPEN_METRIC(metric)                                                                                     \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "reopen/" #metric, kmonitor::GAUGE, "ms")

    INIT_REOPEN_METRIC(reopenIncLatency);
    INIT_REOPEN_METRIC(reopenRealtimeLatency);

    INIT_REOPEN_METRIC(ReclaimRtIndexLatency);
    INIT_REOPEN_METRIC(ReopenFullPartitionLatency);

    INIT_REOPEN_METRIC(decisionLatency);
    INIT_REOPEN_METRIC(preloadLatency);
    INIT_REOPEN_METRIC(SwitchFlushRtSegmentLatency);
    INIT_REOPEN_METRIC(LockedDumpContainerFlushLatency);
    INIT_REOPEN_METRIC(UnlockedDumpContainerFlushLatency);

    if (tableType != tt_kv && tableType != tt_kkv && tableType != tt_customized) {
        INIT_REOPEN_METRIC(GenerateJoinSegmentLatency);
        INIT_REOPEN_METRIC(createJoinSegmentWriterLatency);
        INIT_REOPEN_METRIC(prejoinLatency);
        INIT_REOPEN_METRIC(prepatchLatency);
        INIT_REOPEN_METRIC(LoadReaderPatchLatency);
        INIT_REOPEN_METRIC(ReclaimReaderMemLatency);
        INIT_REOPEN_METRIC(RedoLatency);
        INIT_REOPEN_METRIC(LockedRedoLatency);
        INIT_REOPEN_METRIC(SwitchBranchLatency);
    } else {
        INIT_REOPEN_METRIC(ReclaimRtSegmentLatency);
    }

    INIT_REOPEN_METRIC(incVersionFreshness);

#undef INIT_REOPEN_METRIC
}

void OnlinePartitionMetrics::RegisterBuildMetrics()
{
    IE_INIT_METRIC_GROUP(mMetricProvider, obsoleteDocQps, "build/obsoleteDocQps", kmonitor::QPS, "count");
}

void OnlinePartitionMetrics::RegisterMetrics(TableType tableType)
{
    RegisterOnlineMetrics();
    RegisterReOpenMetrics(tableType);
    RegisterBuildMetrics();
    mAttributeMetrics.RegisterMetrics(tableType);
    mIndexMetrics.RegisterMetrics(tableType);
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
    IE_REPORT_METRIC(partitionMemoryQuotaUseRatio, mpartitionMemoryQuotaUseRatio);
    IE_REPORT_METRIC(freeMemoryQuotaRatio, mfreeMemoryQuotaRatio);

    IE_REPORT_METRIC(missingSegment, mmissingSegmentCount > 0 ? 1 : 0);
    IE_REPORT_METRIC(builtRtIndexMemoryUse, mbuiltRtIndexMemoryUse);
    IE_REPORT_METRIC(buildingSegmentMemoryUse, mbuildingSegmentMemoryUse);
    IE_REPORT_METRIC(oldInMemorySegmentMemoryUse, moldInMemorySegmentMemoryUse);
    IE_REPORT_METRIC(partitionMemoryQuotaUse, mpartitionMemoryQuotaUse);
    IE_REPORT_METRIC(totalMemoryQuotaLimit, mtotalMemoryQuotaLimit);
    IE_REPORT_METRIC(freeMemoryQuota, mfreeMemoryQuota);
    IE_REPORT_METRIC(partitionReaderVersionCount, mpartitionReaderVersionCount);
    IE_REPORT_METRIC(latestReaderVersionId, mlatestReaderVersionId);
    IE_REPORT_METRIC(oldestReaderVersionId, moldestReaderVersionId);
    IE_REPORT_METRIC(missingSegmentCount, mmissingSegmentCount);
    IE_REPORT_METRIC(inMemRtSegmentCount, minMemRtSegmentCount);
    IE_REPORT_METRIC(onDiskRtSegmentCount, monDiskRtSegmentCount);
    IE_REPORT_METRIC(usedOnDiskRtSegmentCount, musedOnDiskRtSegmentCount);
    IE_REPORT_METRIC(incVersionFreshness, mincVersionFreshness);
    IE_REPORT_METRIC(RedoOperationLockRealtimeTimeused, mRedoOperationLockRealtimeTimeused);
}

void OnlinePartitionMetrics::ReportMetrics()
{
    ReportOnlineMetrics();
    mAttributeMetrics.ReportMetrics();
    mIndexMetrics.ReportMetrics();
}

void OnlinePartitionMetrics::IncreateObsoleteDocQps() { IE_INCREASE_QPS(obsoleteDocQps); }

void OnlinePartitionMetrics::PrintMetrics(const OnlineConfig& onlineConfig, const string& partitionName)
{
    int64_t currentTsInSeconds = TimeUtility::currentTimeInSeconds();
    if (currentTsInSeconds - mPrintMetricsTsInSeconds > onlineConfig.printMetricsInterval) {
        IE_LOG(INFO,
               "Print OnlinePartitionMetrics for table [%s], "
               "partitionIndexSize [%ld], partitionMemoryUse [%ld], incIndexMemoryUse [%ld], "
               "rtIndexMemoryUse [%ld], builtRtIndexMemoryUse [%ld], buildingSegmentMemoryUse [%ld], "
               "oldInMemorySegmentMemoryUse [%ld], partitionMemoryQuotaUse [%ld], "
               "partitionReaderVersionCount [%ld], "
               "latestReaderVersionId [%d], oldestReaderVersionId [%d], missingSegmentCount [%ld]",
               partitionName.c_str(), mpartitionIndexSize, mpartitionMemoryUse, mincIndexMemoryUse, mrtIndexMemoryUse,
               mbuiltRtIndexMemoryUse, mbuildingSegmentMemoryUse, moldInMemorySegmentMemoryUse,
               mpartitionMemoryQuotaUse, mpartitionReaderVersionCount, mlatestReaderVersionId, moldestReaderVersionId,
               mmissingSegmentCount);

        mPrintMetricsTsInSeconds = currentTsInSeconds;
    }
}
}} // namespace indexlib::partition
