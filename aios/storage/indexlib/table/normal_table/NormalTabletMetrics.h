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

namespace indexlib::util {
class CounterMap;
class AccumulativeCounter;
} // namespace indexlib::util

namespace indexlibv2::config {
class ITabletSchema;
}

namespace indexlibv2::framework {
class MetricsManager;
}

namespace indexlibv2::table {

class NormalTabletMetrics
{
public:
    explicit NormalTabletMetrics(framework::MetricsManager* metricsManager);
    NormalTabletMetrics(const NormalTabletMetrics& other);
    ~NormalTabletMetrics();

public:
    void Init(const std::shared_ptr<config::ITabletSchema>& schema);
    void* GetSchemaSignature() const;

    void RegisterAttributeAccess(const std::string& indexName);
    void RegisterInvertedAccess(const std::string& indexName);
    void IncAttributeAccess(const std::string& indexName);
    void IncInvertedAccess(const std::string& indexName);

    typedef std::unordered_map<std::string, std::shared_ptr<indexlib::util::AccumulativeCounter>> AccessCounterMap;
    const AccessCounterMap& GetAttributeAccessCounter() const;
    const AccessCounterMap& GetInvertedAccessCounter() const;

private:
    std::string _tabletName;
    std::shared_ptr<indexlib::util::CounterMap> _counterMap;

    void* schemaSignature = nullptr;
    AccessCounterMap _attributeAccessCounter;
    AccessCounterMap _invertedAccessCounter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table
