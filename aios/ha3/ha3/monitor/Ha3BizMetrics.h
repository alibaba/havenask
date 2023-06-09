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

#include <memory>

#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"

namespace isearch {
namespace monitor {
class SessionMetricsCollector;
}  // namespace monitor
}  // namespace isearch
namespace kmonitor {
class MetricsTags;
class MutableMetric;
}  // namespace kmonitor

namespace isearch {
namespace monitor {

class Ha3BizMetrics : public kmonitor::MetricsGroup
{
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, SessionMetricsCollector *metricCollector);
public:
    kmonitor::MutableMetric *_requestPoolWaitTime = nullptr;
    kmonitor::MutableMetric *_memPoolUsedBufferSize = nullptr;
    kmonitor::MutableMetric *_memPoolAllocatedBufferSize = nullptr;
};

typedef std::shared_ptr<Ha3BizMetrics> Ha3BizMetricsPtr;

#define HA3_REPORT_MUTABLE_METRIC(metric, value) \
    if (value >=0 ) {                            \
        REPORT_MUTABLE_METRIC(metric, value);    \
    }                                            \


} // namespace monitor
} // namespace isearch
