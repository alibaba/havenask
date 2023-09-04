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

#include "autil/Log.h"
#include "indexlib/index/attribute/AttributeMetrics.h"

namespace indexlib::util {
class CounterMap;
class AccumulativeCounter;
} // namespace indexlib::util

namespace indexlibv2::framework {
class MetricsManager;
}

namespace indexlibv2::index {
class PackAttributeConfig;

class PackAttributeMetrics : public AttributeMetrics
{
public:
    explicit PackAttributeMetrics(framework::MetricsManager* metricsManager);
    ~PackAttributeMetrics() = default;

public:
    void RegisterAttributeAccess(const std::shared_ptr<PackAttributeConfig> packAttributeConfig);
    void IncAttributeAccess(const std::string& subAttrName);

private:
    void RegisterAttributeAccess(const std::string& subAttrName);

private:
    // INDEXLIB_FM_DECLARE_NORMAL_METRIC(int64_t, packAttributeReaderBufferSize);
    // std::shared_ptr<kmonitor::MetricsReporter> _metrics;

    typedef std::unordered_map<std::string, std::shared_ptr<indexlib::util::AccumulativeCounter>> AccessCounterMap;
    std::shared_ptr<indexlib::util::CounterMap> _counterMap;
    AccessCounterMap _attributeAccessCounter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
