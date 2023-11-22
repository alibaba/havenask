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

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/framework/IMetrics.h"

namespace kmonitor {
class MetricsReporter;
}

namespace indexlib::util {
class CounterMap;
}

namespace indexlibv2::framework {

class MetricsManager : public autil::NoCopyable
{
public:
    MetricsManager(const std::string& tabletName, const std::shared_ptr<kmonitor::MetricsReporter>& metricsReporter);
    ~MetricsManager() = default;

    const std::string& GetTabletName() const { return _tabletName; }

public:
    // metris
    std::shared_ptr<IMetrics> CreateMetrics(const std::string& identifier,
                                            const std::function<std::shared_ptr<IMetrics>()>& creator);
    void ReportMetrics();
    std::shared_ptr<kmonitor::MetricsReporter> GetMetricsReporter() const { return _metricsReporter; }

public:
    // counter
    const std::shared_ptr<indexlib::util::CounterMap>& GetCounterMap() const { return _counterMap; }

private:
    void RecycleMetricsUnSafe();

private:
    std::mutex _metriceMutex;
    std::string _tabletName;
    std::shared_ptr<kmonitor::MetricsReporter> _metricsReporter;
    std::map<std::string, std::shared_ptr<IMetrics>> _metricsMap;
    std::shared_ptr<indexlib::util::CounterMap> _counterMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework
