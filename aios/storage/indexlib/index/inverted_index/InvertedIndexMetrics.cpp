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
#include "indexlib/index/inverted_index/InvertedIndexMetrics.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/framework/MetricsManager.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, InvertedIndexMetrics);

InvertedIndexMetrics::InvertedIndexMetrics(const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter)
    : _metricsReporter(metricsReporter)
{
}

void InvertedIndexMetrics::RegisterMetrics()
{
#define REGISTER_INVERTED_INDEX_METRIC(metricName)                                                                     \
    REGISTER_METRIC_WITH_INDEXLIB_PREFIX(_metricsReporter, metricName, "inverted_index/" #metricName, kmonitor::GAUGE)

    REGISTER_INVERTED_INDEX_METRIC(DynamicIndexDocCount);
    REGISTER_INVERTED_INDEX_METRIC(DynamicIndexMemory);
    REGISTER_INVERTED_INDEX_METRIC(DynamicIndexBuildingMemory);
    REGISTER_INVERTED_INDEX_METRIC(DynamicIndexRetiredMemory);
    REGISTER_INVERTED_INDEX_METRIC(DynamicIndexBuildingRetiredMemory);
    REGISTER_INVERTED_INDEX_METRIC(DynamicIndexTreeCount);

    // debug
    REGISTER_INVERTED_INDEX_METRIC(DynamicIndexTotalAllocatedMemory);
    REGISTER_INVERTED_INDEX_METRIC(DynamicIndexTotalFreedMemory);

    REGISTER_INVERTED_INDEX_METRIC(SortDumpReorderTimeUS);
    REGISTER_INVERTED_INDEX_METRIC(SortDumpReorderMaxMemUse);

#undef REGISTER_INVERTED_INDEX_METRIC
}

void InvertedIndexMetrics::ReportMetrics()
{
    std::lock_guard<std::mutex> lg(_mtx);
    for (const auto& kvPair : _indexMetrics) {
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, DynamicIndexDocCount,
                                                      kvPair.second->dynamicIndexDocCount);
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, DynamicIndexMemory,
                                                      kvPair.second->dynamicIndexMemory);
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, DynamicIndexBuildingMemory,
                                                      kvPair.second->dynamicIndexBuildingMemory);
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, DynamicIndexRetiredMemory,
                                                      kvPair.second->dynamicIndexRetiredMemory);
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, DynamicIndexBuildingRetiredMemory,
                                                      kvPair.second->dynamicIndexBuildingRetiredMemory);
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, DynamicIndexTreeCount,
                                                      kvPair.second->dynamicIndexTreeCount);

        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, DynamicIndexTotalAllocatedMemory,
                                                      kvPair.second->dynamicIndexTotalAllocatedMemory);
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, DynamicIndexTotalFreedMemory,
                                                      kvPair.second->dynamicIndexTotalFreedMemory);
    }

    for (const auto& kvPair : _sortDumpMetric) {
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, SortDumpReorderTimeUS,
                                                      kvPair.second->sortDumpTime);
        INDEXLIB_FM_REPORT_METRIC_WITH_TAGS_AND_VALUE(&kvPair.second->tags, SortDumpReorderMaxMemUse,
                                                      kvPair.second->maxMemUse);
    }
}

InvertedIndexMetrics::SingleFieldIndexMetrics* InvertedIndexMetrics::AddSingleFieldIndex(const std::string& indexName)
{
    std::lock_guard<std::mutex> lg(_mtx);
    auto iter = _indexMetrics.find(indexName);
    if (iter == _indexMetrics.end()) {
        auto singleFieldMetrics = std::make_unique<SingleFieldIndexMetrics>();
        singleFieldMetrics->tags.AddTag("index_name", indexName);
        iter = _indexMetrics.insert(iter, {indexName, std::move(singleFieldMetrics)});
        AUTIL_LOG(INFO, "create index metrics for [%s]", indexName.c_str());
    }
    AUTIL_LOG(INFO, "get index metrics for [%s]", indexName.c_str());
    return iter->second.get();
}

InvertedIndexMetrics::SortDumpMetrics* InvertedIndexMetrics::AddSingleFieldSortDumpMetric(const std::string& indexName)
{
    std::lock_guard<std::mutex> lg(_mtx);
    auto iter = _sortDumpMetric.find(indexName);
    if (iter == _sortDumpMetric.end()) {
        auto sortDumpMetrics = std::make_unique<SortDumpMetrics>();
        sortDumpMetrics->tags.AddTag("index_name", indexName);
        iter = _sortDumpMetric.insert(iter, {indexName, std::move(sortDumpMetrics)});
        AUTIL_LOG(INFO, "create index sort dump metrics for [%s]", indexName.c_str());
    } else {
        iter->second->Reset();
        AUTIL_LOG(INFO, "reset index sort dump metrics for [%s]", indexName.c_str());
    }
    return iter->second.get();
}

InvertedIndexMetrics::SortDumpMetrics*
InvertedIndexMetrics::TEST_GetSingleFieldSortDumpMetric(const std::string& indexName)
{
    std::lock_guard<std::mutex> lg(_mtx);
    auto iter = _sortDumpMetric.find(indexName);
    if (iter == _sortDumpMetric.end()) {
        return nullptr;
    }
    return iter->second.get();
}

std::shared_ptr<InvertedIndexMetrics>
InvertedIndexMetrics::Create(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                             indexlibv2::framework::MetricsManager* metricsManager)
{
    if (!metricsManager) {
        return nullptr;
    }

    assert(metricsManager);
    std::string identifier = "__inverted_index_" + indexConfig->GetIndexName();
    std::shared_ptr<InvertedIndexMetrics> invertedIndexMetrics =
        std::dynamic_pointer_cast<InvertedIndexMetrics>(metricsManager->CreateMetrics(
            identifier, [metricsManager]() -> std::shared_ptr<indexlibv2::framework::IMetrics> {
                return std::make_shared<InvertedIndexMetrics>(metricsManager->GetMetricsReporter());
            }));
    assert(invertedIndexMetrics);
    return invertedIndexMetrics;
}

} // namespace indexlib::index
