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

#include <map>
#include <memory>
#include <string>

#include "suez/sdk/IndexProvider.h"
#include "suez/sdk/KMonitorMetaInfo.h"
#include "suez/sdk/PartitionId.h"

namespace indexlib {
namespace util {
class CounterMap;
using CounterMapPtr = std::shared_ptr<CounterMap>;
} // namespace util
} // namespace indexlib

namespace kmonitor {
class MetricsReporter;
using MetricsReporterPtr = std::shared_ptr<MetricsReporter>;
} // namespace kmonitor

namespace suez {

class CounterReporter {
public:
    CounterReporter();
    ~CounterReporter();

public:
    void init(const KMonitorMetaInfo &kmonMetaInfo);
    void report(const IndexProvider &indexProvider);

private:
    void reportOneTableMetrics(const PartitionId &partId, const indexlib::util::CounterMapPtr &counterMap);
    static void convertCounterPath(const std::string &counterPath,
                                   std::string &metricName,
                                   std::map<std::string, std::string> &tagsMap);

private:
    kmonitor::MetricsReporterPtr _metricsReporter;
};

} // namespace suez
