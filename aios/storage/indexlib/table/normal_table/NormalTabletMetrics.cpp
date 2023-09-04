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
#include "indexlib/table/normal_table/NormalTabletMetrics.h"

#include "indexlib/config/IIndexConfig.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/attribute/Common.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/index/inverted_index/Common.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTabletMetrics);

#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] [%p] " format, _tabletName.c_str(), this, ##args)

NormalTabletMetrics::NormalTabletMetrics(framework::MetricsManager* metricsManager)
{
    if (metricsManager) {
        _tabletName = metricsManager->GetTabletName();
        _counterMap = metricsManager->GetCounterMap();
    }
}

NormalTabletMetrics::NormalTabletMetrics(const NormalTabletMetrics& other)
    : _tabletName(other._tabletName)
    , _counterMap(other._counterMap)
{
}

NormalTabletMetrics::~NormalTabletMetrics() {}

void* NormalTabletMetrics::GetSchemaSignature() const { return schemaSignature; }

void NormalTabletMetrics::Init(const std::shared_ptr<config::ITabletSchema>& schema)
{
    schemaSignature = schema.get();
    for (const auto& attributeConfig : schema->GetIndexConfigs(indexlib::index::GENERAL_VALUE_INDEX_TYPE_STR)) {
        RegisterAttributeAccess(attributeConfig->GetIndexName());
    }
    for (const auto& invertedConfig : schema->GetIndexConfigs(indexlib::index::GENERAL_INVERTED_INDEX_TYPE_STR)) {
        RegisterInvertedAccess(invertedConfig->GetIndexName());
    }
}

void NormalTabletMetrics::RegisterAttributeAccess(const std::string& indexName)
{
    if (!_counterMap || _attributeAccessCounter.count(indexName) > 0) {
        return;
    }
    std::string counterNodePath = "online.access.attribute." + indexName;
    const auto& counter = _counterMap->GetAccCounter(counterNodePath);
    if (!counter) {
        TABLET_LOG(ERROR, "get counter[%s] failed", counterNodePath.c_str());
        return;
    }
    _attributeAccessCounter.emplace(indexName, counter);
}

void NormalTabletMetrics::RegisterInvertedAccess(const std::string& indexName)
{
    if (!_counterMap || _invertedAccessCounter.count(indexName) > 0) {
        return;
    }
    std::string counterNodePath = "online.access.inverted_index." + indexName;
    const auto& counter = _counterMap->GetAccCounter(counterNodePath);
    if (!counter) {
        TABLET_LOG(ERROR, "get counter[%s] failed", counterNodePath.c_str());
        return;
    }
    _invertedAccessCounter.emplace(indexName, counter);
}

void NormalTabletMetrics::IncAttributeAccess(const std::string& indexName)
{
    AccessCounterMap::iterator iter = _attributeAccessCounter.find(indexName);
    if (iter != _attributeAccessCounter.end() && iter->second != nullptr) {
        iter->second->Increase(1);
    }
}

void NormalTabletMetrics::IncInvertedAccess(const std::string& indexName)
{
    AccessCounterMap::iterator iter = _invertedAccessCounter.find(indexName);
    if (iter != _invertedAccessCounter.end() && iter->second != nullptr) {
        iter->second->Increase(1);
    }
}

const NormalTabletMetrics::AccessCounterMap& NormalTabletMetrics::GetAttributeAccessCounter() const
{
    return _attributeAccessCounter;
}
const NormalTabletMetrics::AccessCounterMap& NormalTabletMetrics::GetInvertedAccessCounter() const
{
    return _invertedAccessCounter;
}

} // namespace indexlibv2::table
