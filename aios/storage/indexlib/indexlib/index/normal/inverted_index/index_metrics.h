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
#pragma once

#include <memory>
#include <mutex>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/util/index_metrics_base.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class IndexMetrics : public IndexMetricsBase
{
public:
    struct SingleFieldIndexMetrics {
        SingleFieldIndexMetrics() = default;
        kmonitor::MetricsTags tags;
        int64_t dynamicIndexDocCount = 0;
        int64_t dynamicIndexMemory = 0;
        int64_t dynamicIndexBuildingMemory = 0;
        int64_t dynamicIndexRetiredMemory = 0;
        int64_t dynamicIndexBuildingRetiredMemory = 0;
        int64_t dynamicIndexTreeCount = 0;

        // debug
        int64_t dynamicIndexTotalAllocatedMemory = 0;
        int64_t dynamicIndexTotalFreedMemory = 0;
    };

public:
    IndexMetrics(util::MetricProviderPtr metricProvider = util::MetricProviderPtr());
    ~IndexMetrics();

    void RegisterMetrics(TableType tableType);

    SingleFieldIndexMetrics* AddSingleFieldIndex(const std::string& indexName);
    void ReportMetrics();

private:
    std::mutex _mtx;
    util::MetricProviderPtr _metricProvider;

    // DynamicIndex
    util::MetricPtr _dynamicIndexDocCountMetric;
    util::MetricPtr _dynamicIndexMemoryMetric;
    util::MetricPtr _dynamicIndexBuildingMemoryMetric;
    util::MetricPtr _dynamicIndexRetiredMemoryMetric;
    util::MetricPtr _dynamicIndexBuildingRetiredMemoryMetric;
    util::MetricPtr _dynamicIndexTreeCountMetric;

    // debug
    util::MetricPtr _dynamicIndexTotalAllocatedMemoryMetric;
    util::MetricPtr _dynamicIndexTotalFreedMemoryMetric;

    std::map<std::string, std::unique_ptr<SingleFieldIndexMetrics>> _indexMetrics;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(IndexMetrics);
}} // namespace indexlib::index
