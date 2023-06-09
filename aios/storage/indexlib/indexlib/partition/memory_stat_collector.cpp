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
#include "indexlib/partition/memory_stat_collector.h"

#include "indexlib/file_system/FileBlockCacheContainer.h"
#include "indexlib/partition/group_memory_reporter.h"
#include "indexlib/partition/online_partition_metrics.h"
#include "indexlib/util/cache/SearchCache.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, MemoryStatCollector);

MemoryStatCollector::MemoryStatCollector(const util::SearchCachePtr& searchCache,
                                         const file_system::FileBlockCacheContainerPtr& blockCacheContainer,
                                         util::MetricProviderPtr metricProvider)
    : mSearchCache(searchCache)
    , mBlockCacheContainer(blockCacheContainer)
    , mMetricProvider(metricProvider)
    , mMetricsInitialized(false)
{
}

void MemoryStatCollector::AddTableMetrics(const std::string& partitionGroupName,
                                          const OnlinePartitionMetricsPtr& tableMetrics)
{
    ScopedLock lock(mLock);

    InnerInitMetrics();
    mMetricsVec.push_back(make_pair(partitionGroupName, tableMetrics));
    if (mMetricProvider && !partitionGroupName.empty()) {
        if (!GetGroupMemoryReporter(partitionGroupName)) {
            GroupMemoryReporterPtr groupReporter(new GroupMemoryReporter(mMetricProvider, partitionGroupName));
            mGroupReporterMap[partitionGroupName] = groupReporter;
        }
    }
}

GroupMemoryReporterPtr MemoryStatCollector::GetGroupMemoryReporter(const string& groupName)
{
    ScopedLock lock(mLock);
    GroupReporterMap::const_iterator iter = mGroupReporterMap.find(groupName);
    if (iter == mGroupReporterMap.end()) {
        return GroupMemoryReporterPtr();
    }
    return iter->second;
}

void MemoryStatCollector::PrintMetrics()
{
    int64_t searchCacheMemoryUse = 0;
    int64_t blockCacheMemoryUse = 0;
    if (mSearchCache) {
        searchCacheMemoryUse = mSearchCache->GetUsage();
    }
    if (mBlockCacheContainer) {
        blockCacheMemoryUse = mBlockCacheContainer->GetResourceInfo().memoryUse;
    }

    InnerUpdateMetrics();

    IE_LOG(INFO,
           "Print total memory status for [%lu] tables, "
           "global searchCacheMemoryUse [%ld], global blockCacheMemoryUse [%ld], "
           "partitionIndexSize [%ld], partitionMemoryUse [%ld], incIndexMemoryUse [%ld], "
           "rtIndexMemoryUse [%ld], builtRtIndexMemoryUse [%ld], buildingSegmentMemoryUse [%ld], "
           "oldInMemorySegmentMemoryUse [%ld], partitionMemoryQuotaUse [%ld]",
           mMetricsVec.size(), searchCacheMemoryUse, blockCacheMemoryUse, mtotalPartitionIndexSize,
           mtotalPartitionMemoryUse, mtotalIncIndexMemoryUse, mtotalRtIndexMemoryUse, mtotalBuiltRtIndexMemoryUse,
           mtotalBuildingSegmentMemoryUse, mtotalOldInMemorySegmentMemoryUse, mtotalPartitionMemoryQuotaUse);

    cerr << "searchCacheMemoryUse: " << searchCacheMemoryUse << endl
         << "blockCacheMemoryUse: " << blockCacheMemoryUse << endl
         << "partitionIndexSize: " << mtotalPartitionIndexSize << endl
         << "partitionMemoryUse: " << mtotalPartitionMemoryUse << endl
         << "incIndexMemoryUse: " << mtotalIncIndexMemoryUse << endl
         << "rtIndexMemoryUse: " << mtotalRtIndexMemoryUse << endl
         << "builtRtIndexMemoryUse: " << mtotalBuiltRtIndexMemoryUse << endl
         << "buildingSegmentMemoryUse: " << mtotalBuildingSegmentMemoryUse << endl
         << "oldInMemorySegmentMemoryUse: " << mtotalOldInMemorySegmentMemoryUse << endl
         << "partitionMemoryQuotaUse: " << mtotalPartitionMemoryQuotaUse << endl;
}

void MemoryStatCollector::ReportMetrics()
{
    if (!mMetricProvider) {
        return;
    }

    InnerUpdateMetrics();

    IE_REPORT_METRIC(totalPartitionIndexSize, mtotalPartitionIndexSize);
    IE_REPORT_METRIC(totalPartitionMemoryUse, mtotalPartitionMemoryUse);
    IE_REPORT_METRIC(totalIncIndexMemoryUse, mtotalIncIndexMemoryUse);
    IE_REPORT_METRIC(totalRtIndexMemoryUse, mtotalRtIndexMemoryUse);
    IE_REPORT_METRIC(totalBuiltRtIndexMemoryUse, mtotalBuiltRtIndexMemoryUse);
    IE_REPORT_METRIC(totalBuildingSegmentMemoryUse, mtotalBuildingSegmentMemoryUse);
    IE_REPORT_METRIC(totalOldInMemorySegmentMemoryUse, mtotalOldInMemorySegmentMemoryUse);
    IE_REPORT_METRIC(totalPartitionMemoryQuotaUse, mtotalPartitionMemoryQuotaUse);

    ScopedLock lock(mLock);
    GroupReporterMap::const_iterator iter = mGroupReporterMap.begin();
    for (; iter != mGroupReporterMap.end(); iter++) {
        iter->second->ReportMetrics();
    }
}

void MemoryStatCollector::InnerUpdateMetrics()
{
    ScopedLock lock(mLock);

    vector<MetricsItem> metricVec;
    metricVec.reserve(mMetricsVec.size());

    mtotalPartitionIndexSize = 0;
    mtotalPartitionMemoryUse = 0;
    mtotalIncIndexMemoryUse = 0;
    mtotalRtIndexMemoryUse = 0;
    mtotalBuiltRtIndexMemoryUse = 0;
    mtotalBuildingSegmentMemoryUse = 0;
    mtotalOldInMemorySegmentMemoryUse = 0;
    mtotalPartitionMemoryQuotaUse = 0;

    GroupReporterMap::const_iterator iter = mGroupReporterMap.begin();
    for (; iter != mGroupReporterMap.end(); iter++) {
        iter->second->Reset();
    }

    for (size_t i = 0; i < mMetricsVec.size(); i++) {
        if (mMetricsVec[i].second.use_count() == 1) {
            // removed partition
            continue;
        }
        metricVec.push_back(mMetricsVec[i]);

        const partition::OnlinePartitionMetricsPtr& partMetrics = mMetricsVec[i].second;
        mtotalPartitionIndexSize += partMetrics->mpartitionIndexSize;
        mtotalPartitionMemoryUse += partMetrics->mpartitionMemoryUse;
        mtotalIncIndexMemoryUse += partMetrics->mincIndexMemoryUse;
        mtotalRtIndexMemoryUse += partMetrics->mrtIndexMemoryUse;
        mtotalBuiltRtIndexMemoryUse += partMetrics->mbuiltRtIndexMemoryUse;
        mtotalBuildingSegmentMemoryUse += partMetrics->mbuildingSegmentMemoryUse;
        mtotalOldInMemorySegmentMemoryUse += partMetrics->moldInMemorySegmentMemoryUse;
        mtotalPartitionMemoryQuotaUse += partMetrics->mpartitionMemoryQuotaUse;

        UpdateGroupMetrics(mMetricsVec[i].first, partMetrics);
    }
    mMetricsVec.swap(metricVec);
}

void MemoryStatCollector::UpdateGroupMetrics(const string& groupName, const OnlinePartitionMetricsPtr& partMetrics)
{
    if (groupName.empty()) {
        return;
    }

    GroupReporterMap::const_iterator iter = mGroupReporterMap.find(groupName);
    if (iter == mGroupReporterMap.end()) {
        return;
    }

    const GroupMemoryReporterPtr& groupReporter = iter->second;
    if (groupReporter) {
        groupReporter->IncreasetotalPartitionIndexSizeValue(partMetrics->mpartitionIndexSize);
        groupReporter->IncreasetotalPartitionMemoryUseValue(partMetrics->mpartitionMemoryUse);
        groupReporter->IncreasetotalIncIndexMemoryUseValue(partMetrics->mincIndexMemoryUse);
        groupReporter->IncreasetotalRtIndexMemoryUseValue(partMetrics->mrtIndexMemoryUse);
        groupReporter->IncreasetotalBuiltRtIndexMemoryUseValue(partMetrics->mbuiltRtIndexMemoryUse);
        groupReporter->IncreasetotalBuildingSegmentMemoryUseValue(partMetrics->mbuildingSegmentMemoryUse);
        groupReporter->IncreasetotalOldInMemorySegmentMemoryUseValue(partMetrics->moldInMemorySegmentMemoryUse);
        groupReporter->IncreasetotalPartitionMemoryQuotaUseValue(partMetrics->mpartitionMemoryQuotaUse);
    }
}

void MemoryStatCollector::InnerInitMetrics()
{
    if (mMetricsInitialized) {
        return;
    }

#define INIT_MEM_STAT_METRIC(metric, unit)                                                                             \
    m##metric = 0L;                                                                                                    \
    IE_INIT_METRIC_GROUP(mMetricProvider, metric, "global/" #metric, kmonitor::GAUGE, unit);

    INIT_MEM_STAT_METRIC(totalPartitionIndexSize, "byte");
    INIT_MEM_STAT_METRIC(totalPartitionMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(totalIncIndexMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(totalRtIndexMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(totalBuiltRtIndexMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(totalBuildingSegmentMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(totalOldInMemorySegmentMemoryUse, "byte");
    INIT_MEM_STAT_METRIC(totalPartitionMemoryQuotaUse, "byte");
#undef INIT_MEM_STAT_METRIC

    mMetricsInitialized = true;
}
}} // namespace indexlib::partition
