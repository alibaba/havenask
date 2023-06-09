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
#ifndef __INDEXLIB_MEMORY_STAT_COLLECTOR_H
#define __INDEXLIB_MEMORY_STAT_COLLECTOR_H

#include <memory>
#include <unordered_map>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

DECLARE_REFERENCE_CLASS(partition, GroupMemoryReporter);
DECLARE_REFERENCE_CLASS(partition, OnlinePartitionMetrics);
DECLARE_REFERENCE_CLASS(util, SearchCache);
DECLARE_REFERENCE_CLASS(file_system, FileBlockCacheContainer);

namespace indexlib { namespace partition {

class MemoryStatCollector
{
public:
    MemoryStatCollector(const util::SearchCachePtr& searchCache,
                        const file_system::FileBlockCacheContainerPtr& blockCacheContainer,
                        util::MetricProviderPtr metricProvider = util::MetricProviderPtr());

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
    void UpdateGroupMetrics(const std::string& groupName, const partition::OnlinePartitionMetricsPtr& partMetrics);

private:
    typedef std::pair<std::string, partition::OnlinePartitionMetricsPtr> MetricsItem;
    typedef std::unordered_map<std::string, GroupMemoryReporterPtr> GroupReporterMap;

    autil::RecursiveThreadMutex mLock;
    std::vector<MetricsItem> mMetricsVec;
    util::SearchCachePtr mSearchCache;
    file_system::FileBlockCacheContainerPtr mBlockCacheContainer;
    util::MetricProviderPtr mMetricProvider;
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
}} // namespace indexlib::partition

#endif //__INDEXLIB_MEMORY_STAT_COLLECTOR_H
