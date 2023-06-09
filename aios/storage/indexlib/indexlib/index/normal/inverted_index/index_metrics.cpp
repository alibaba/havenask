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
#include "indexlib/index/normal/inverted_index/index_metrics.h"

using namespace std;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, IndexMetrics);

IndexMetrics::IndexMetrics(util::MetricProviderPtr metricProvider) : _metricProvider(std::move(metricProvider)) {}

IndexMetrics::~IndexMetrics() {}

void IndexMetrics::RegisterMetrics(TableType tableType)
{
    std::lock_guard<std::mutex> lg(_mtx);
    if (tableType == tt_kv or tableType == tt_kkv) {
        return;
    }
    auto initIndexMetric = [this](const std::string& metricName, const std::string& unit) mutable -> util::MetricPtr {
        if (!_metricProvider) {
            return nullptr;
        }
        std::string declareMetricName = std::string("indexlib/inverted_index/") + metricName;
        return _metricProvider->DeclareMetric(declareMetricName, kmonitor::STATUS);
    };

    _dynamicIndexDocCountMetric = initIndexMetric("DynamicIndexDocCount", "count");
    _dynamicIndexMemoryMetric = initIndexMetric("DynamicIndexMemory", "byte");
    _dynamicIndexBuildingMemoryMetric = initIndexMetric("DynamicIndexBuildingMemory", "byte");
    _dynamicIndexRetiredMemoryMetric = initIndexMetric("DynamicIndexRetiredMemory", "byte");
    _dynamicIndexBuildingRetiredMemoryMetric = initIndexMetric("DynamicIndexBuildingRetiredMemory", "byte");
    _dynamicIndexTreeCountMetric = initIndexMetric("DynamicIndexTreeCount", "count");

    _dynamicIndexTotalAllocatedMemoryMetric = initIndexMetric("DynamicIndexTotalAllocatedMemory", "byte");
    _dynamicIndexTotalFreedMemoryMetric = initIndexMetric("DynamicIndexTotalFreedMemory", "byte");
}

IndexMetrics::SingleFieldIndexMetrics* IndexMetrics::AddSingleFieldIndex(const std::string& indexName)
{
    std::lock_guard<std::mutex> lg(_mtx);
    auto iter = _indexMetrics.find(indexName);
    if (iter == _indexMetrics.end()) {
        auto singleFieldMetrics = std::make_unique<SingleFieldIndexMetrics>();
        singleFieldMetrics->tags.AddTag("index_name", indexName);
        iter = _indexMetrics.insert(iter, {indexName, std::move(singleFieldMetrics)});
        IE_LOG(INFO, "create index metrics for [%s]", indexName.c_str());
    }
    IE_LOG(INFO, "get index metrics for [%s]", indexName.c_str());
    return iter->second.get();
}
void IndexMetrics::ReportMetrics()
{
    auto report = [](util::Metric* metric, const kmonitor::MetricsTags* tags, int64_t value) {
        if (metric) {
            metric->Report(tags, value);
        }
    };

    std::lock_guard<std::mutex> lg(_mtx);
    for (const auto& kvPair : _indexMetrics) {
        report(_dynamicIndexBuildingRetiredMemoryMetric.get(), &kvPair.second->tags,
               kvPair.second->dynamicIndexBuildingRetiredMemory);
        report(_dynamicIndexMemoryMetric.get(), &kvPair.second->tags, kvPair.second->dynamicIndexMemory);
        report(_dynamicIndexBuildingMemoryMetric.get(), &kvPair.second->tags,
               kvPair.second->dynamicIndexBuildingMemory);
        report(_dynamicIndexRetiredMemoryMetric.get(), &kvPair.second->tags, kvPair.second->dynamicIndexRetiredMemory);
        report(_dynamicIndexBuildingRetiredMemoryMetric.get(), &kvPair.second->tags,
               kvPair.second->dynamicIndexBuildingRetiredMemory);
        report(_dynamicIndexTreeCountMetric.get(), &kvPair.second->tags, kvPair.second->dynamicIndexTreeCount);

        report(_dynamicIndexTotalAllocatedMemoryMetric.get(), &kvPair.second->tags,
               kvPair.second->dynamicIndexTotalAllocatedMemory);
        report(_dynamicIndexTotalFreedMemoryMetric.get(), &kvPair.second->tags,
               kvPair.second->dynamicIndexTotalFreedMemory);
    }
}
}} // namespace indexlib::index
