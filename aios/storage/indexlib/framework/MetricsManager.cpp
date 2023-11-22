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
#include "indexlib/framework/MetricsManager.h"

#include <assert.h>
#include <utility>

#include "indexlib/util/counter/CounterMap.h"
#include "kmonitor/client/MetricsReporter.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, MetricsManager);

MetricsManager::MetricsManager(const std::string& tabletName,
                               const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter)
    : _tabletName(tabletName)
    , _metricsReporter(metricsReporter)
    , _counterMap(std::make_shared<indexlib::util::CounterMap>())
{
}

std::shared_ptr<IMetrics> MetricsManager::CreateMetrics(const std::string& identifier,
                                                        const std::function<std::shared_ptr<IMetrics>()>& creator)
{
    std::lock_guard<std::mutex> guard(_metriceMutex);
    RecycleMetricsUnSafe();

    auto it = _metricsMap.find(identifier);
    if (it != _metricsMap.end()) {
        return it->second;
    }
    auto metrics = creator();
    assert(metrics);
    metrics->RegisterMetrics();
    _metricsMap[identifier] = metrics;
    return _metricsMap[identifier];
}

void MetricsManager::ReportMetrics()
{
    std::lock_guard<std::mutex> guard(_metriceMutex);
    RecycleMetricsUnSafe();
    for (auto it : _metricsMap) {
        it.second->ReportMetrics();
    }
}

void MetricsManager::RecycleMetricsUnSafe()
{
    for (auto it = _metricsMap.begin(); it != _metricsMap.end();) {
        if (it->second.use_count() == 1) {
            it = _metricsMap.erase(it);
            continue;
        }
        it++;
    }
}

} // namespace indexlibv2::framework
