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

#include <mutex>

#include "autil/Log.h"
#include "indexlib/framework/IMetrics.h"
#include "indexlib/framework/MetricsWrapper.h"

namespace kmonitor {
class MetricsReporter;
}

namespace indexlibv2::config {
class IIndexConfig;
}

namespace indexlibv2::framework {
class MetricsManager;
}
namespace indexlib::index {
class InvertedIndexMetrics : public indexlibv2::framework::IMetrics
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

    struct SortDumpMetrics {
        SortDumpMetrics() = default;
        kmonitor::MetricsTags tags;
        int64_t sortDumpTime = 0;
        size_t maxMemUse = 0;
        void Reset()
        {
            sortDumpTime = 0;
            maxMemUse = 0;
        }
    };

    explicit InvertedIndexMetrics(const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter);
    ~InvertedIndexMetrics() = default;

    void RegisterMetrics() override;
    void ReportMetrics() override;

    SingleFieldIndexMetrics* AddSingleFieldIndex(const std::string& indexName);
    SortDumpMetrics* AddSingleFieldSortDumpMetric(const std::string& indexName);

    SortDumpMetrics* TEST_GetSingleFieldSortDumpMetric(const std::string& indexName);

    static std::shared_ptr<InvertedIndexMetrics>
    Create(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
           indexlibv2::framework::MetricsManager* metricsManager);

private:
    std::mutex _mtx;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;

    INDEXLIB_FM_DECLARE_METRIC(DynamicIndexDocCount);
    INDEXLIB_FM_DECLARE_METRIC(DynamicIndexMemory);
    INDEXLIB_FM_DECLARE_METRIC(DynamicIndexBuildingMemory);
    INDEXLIB_FM_DECLARE_METRIC(DynamicIndexRetiredMemory);
    INDEXLIB_FM_DECLARE_METRIC(DynamicIndexBuildingRetiredMemory);
    INDEXLIB_FM_DECLARE_METRIC(DynamicIndexTreeCount);

    // debug
    INDEXLIB_FM_DECLARE_METRIC(DynamicIndexTotalAllocatedMemory);
    INDEXLIB_FM_DECLARE_METRIC(DynamicIndexTotalFreedMemory);

    std::map<std::string, std::unique_ptr<SingleFieldIndexMetrics>> _indexMetrics;

    // sortDumpMetrics
    INDEXLIB_FM_DECLARE_METRIC(SortDumpReorderTimeUS);
    // caculator sort dump progress reorder && construct new posting writer time, us
    INDEXLIB_FM_DECLARE_METRIC(SortDumpReorderMaxMemUse)
    // caculator sort dump progress reorder && construct new posting writer max mem use bytes

    std::map<std::string, std::unique_ptr<SortDumpMetrics>> _sortDumpMetric;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
