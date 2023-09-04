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
#include "indexlib/index/pack_attribute/PackAttributeMetrics.h"

#include "indexlib/framework/MetricsManager.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/pack_attribute/PackAttributeConfig.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, PackAttributeMetrics);

PackAttributeMetrics::PackAttributeMetrics(framework::MetricsManager* metricsManager)
    : AttributeMetrics(metricsManager ? metricsManager->GetMetricsReporter() : nullptr)
{
    if (metricsManager) {
        _counterMap = metricsManager->GetCounterMap();
    }
}

void PackAttributeMetrics::RegisterAttributeAccess(const std::shared_ptr<PackAttributeConfig> packAttributeConfig)
{
    const auto& attributeConfigs = packAttributeConfig->GetAttributeConfigVec();
    for (const auto& attributeConfig : attributeConfigs) {
        RegisterAttributeAccess(attributeConfig->GetAttrName());
    }
}

void PackAttributeMetrics::RegisterAttributeAccess(const std::string& subAttrName)
{
    if (!_counterMap || _attributeAccessCounter.count(subAttrName) > 0) {
        return;
    }
    std::string counterNodePath = "online.access.attribute." + subAttrName;
    const auto& counter = _counterMap->GetAccCounter(counterNodePath);
    if (!counter) {
        AUTIL_LOG(ERROR, "get counter[%s] failed", counterNodePath.c_str());
        return;
    }
    _attributeAccessCounter.emplace(subAttrName, counter);
}

void PackAttributeMetrics::IncAttributeAccess(const std::string& subAttrName)
{
    assert(!_attributeAccessCounter.empty());
    AccessCounterMap::iterator iter = _attributeAccessCounter.find(subAttrName);
    if (iter != _attributeAccessCounter.end() && iter->second != nullptr) {
        iter->second->Increase(1);
    }
}

} // namespace indexlibv2::index
