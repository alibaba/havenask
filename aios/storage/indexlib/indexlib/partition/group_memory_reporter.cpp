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
#include "indexlib/partition/group_memory_reporter.h"

using namespace std;

using namespace indexlib::util;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, GroupMemoryReporter);

GroupMemoryReporter::GroupMemoryReporter(MetricProviderPtr metricProvider, const string& groupName)
{
    assert(metricProvider);

#define INIT_MEM_STAT_METRIC(groupName, metric, unit)                                                                  \
    {                                                                                                                  \
        m##metric = 0L;                                                                                                \
        string metricName = string("group/") + groupName + "/" #metric;                                                \
        IE_INIT_METRIC_GROUP(metricProvider, metric, metricName, kmonitor::GAUGE, unit);                               \
    }

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

GroupMemoryReporter::~GroupMemoryReporter() {}

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
}} // namespace indexlib::partition
