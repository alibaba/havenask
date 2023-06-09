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
#ifndef __INDEXLIB_ONLINE_PARTITION_METRICS_H
#define __INDEXLIB_ONLINE_PARTITION_METRICS_H

#include <memory>

#include "autil/TimeUtility.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/attribute_metrics.h"
#include "indexlib/index/normal/inverted_index/index_metrics.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, OnlineConfig);

namespace indexlib { namespace partition {

class OnlinePartitionMetrics
{
public:
    OnlinePartitionMetrics(util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~OnlinePartitionMetrics();

public:
    void RegisterMetrics(TableType tableType);
    void ReportMetrics();
    void IncreateObsoleteDocQps();

    index::AttributeMetrics* GetAttributeMetrics() { return &mAttributeMetrics; }
    index::IndexMetrics* GetIndexMetrics() { return &mIndexMetrics; }
    util::MetricProviderPtr GetMetricProvider() { return mMetricProvider; }

    void PrintMetrics(const config::OnlineConfig& onlineConfig, const std::string& partitionName);

private:
    void RegisterOnlineMetrics();
    void RegisterReOpenMetrics(TableType tableType);
    void RegisterBuildMetrics();

    void ReportOnlineMetrics();

private:
    util::MetricProviderPtr mMetricProvider;
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
    IE_DECLARE_PARAM_METRIC(double, freeMemoryQuotaRatio);
    IE_DECLARE_PARAM_METRIC(double, partitionMemoryQuotaUseRatio);
    IE_DECLARE_PARAM_METRIC(int64_t, totalMemoryQuotaLimit);
    IE_DECLARE_PARAM_METRIC(int64_t, freeMemoryQuota);

    IE_DECLARE_PARAM_METRIC(int64_t, incVersionFreshness);

    // reopen
    IE_DECLARE_REACHABLE_METRIC(reopenIncLatency);
    IE_DECLARE_REACHABLE_METRIC(reopenRealtimeLatency);
    IE_DECLARE_REACHABLE_METRIC(ReclaimRtIndexLatency);
    IE_DECLARE_REACHABLE_METRIC(ReclaimRtSegmentLatency);
    IE_DECLARE_REACHABLE_METRIC(GenerateJoinSegmentLatency);
    IE_DECLARE_REACHABLE_METRIC(ReopenFullPartitionLatency);
    IE_DECLARE_REACHABLE_METRIC(LoadReaderPatchLatency);
    IE_DECLARE_REACHABLE_METRIC(decisionLatency);
    IE_DECLARE_REACHABLE_METRIC(createJoinSegmentWriterLatency);
    IE_DECLARE_REACHABLE_METRIC(preloadLatency);
    IE_DECLARE_REACHABLE_METRIC(prejoinLatency);
    IE_DECLARE_REACHABLE_METRIC(prepatchLatency);
    IE_DECLARE_REACHABLE_METRIC(ReclaimReaderMemLatency);
    IE_DECLARE_REACHABLE_METRIC(RedoLatency);
    IE_DECLARE_REACHABLE_METRIC(LockedRedoLatency);
    IE_DECLARE_REACHABLE_METRIC(SwitchBranchLatency);
    IE_DECLARE_REACHABLE_METRIC(SwitchFlushRtSegmentLatency);
    IE_DECLARE_REACHABLE_METRIC(LockedDumpContainerFlushLatency);
    IE_DECLARE_REACHABLE_METRIC(UnlockedDumpContainerFlushLatency);

    // build
    IE_DECLARE_REACHABLE_METRIC(obsoleteDocQps);

    // customize
    IE_DECLARE_PARAM_METRIC(int64_t, inMemRtSegmentCount);
    IE_DECLARE_PARAM_METRIC(int64_t, onDiskRtSegmentCount);
    IE_DECLARE_PARAM_METRIC(int64_t, usedOnDiskRtSegmentCount);

    // dump
    IE_DECLARE_REACHABLE_METRIC(RedoOperationLogTimeused);
    IE_DECLARE_PARAM_METRIC(int64_t, RedoOperationLockRealtimeTimeused);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(OnlinePartitionMetrics);
}} // namespace indexlib::partition

#endif //__INDEXLIB_ONLINE_PARTITION_METRICS_H
