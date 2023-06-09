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
#ifndef __INDEXLIB_GROUP_MEMORY_REPORTER_H
#define __INDEXLIB_GROUP_MEMORY_REPORTER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/metrics/Monitor.h"

namespace indexlib { namespace partition {

class GroupMemoryReporter
{
public:
    GroupMemoryReporter(util::MetricProviderPtr metricProvider, const std::string& groupName);
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
}} // namespace indexlib::partition

#endif //__INDEXLIB_GROUP_MEMORY_REPORTER_H
