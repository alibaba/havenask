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
#include "ha3/monitor/Ha3BizMetrics.h"

#include <iosfwd>

#include "alog/Logger.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "kmonitor/client/core/MutableMetric.h"

namespace kmonitor {
class MetricsTags;
}  // namespace kmonitor

using namespace std;
using namespace kmonitor;

namespace isearch {
namespace monitor {

bool Ha3BizMetrics::init(MetricsGroupManager *manager) {
    REGISTER_LATENCY_MUTABLE_METRIC(_requestPoolWaitTime, "basic.requestPoolWaitTime");
    REGISTER_GAUGE_MUTABLE_METRIC(_memPoolUsedBufferSize,"basic.memPoolUsedBufferSize");
    REGISTER_GAUGE_MUTABLE_METRIC(_memPoolAllocatedBufferSize, "basic.memPoolAllocatedBufferSize");
    return true;
}

void Ha3BizMetrics::report(const MetricsTags *tags, SessionMetricsCollector *sessionCollector) {
    REPORT_MUTABLE_METRIC(_requestPoolWaitTime, sessionCollector->getPoolWaitLatency());
    REPORT_MUTABLE_METRIC(_memPoolUsedBufferSize, sessionCollector->getMemPoolUsedBufferSize());
    REPORT_MUTABLE_METRIC(_memPoolAllocatedBufferSize, sessionCollector->getMemPoolAllocatedBufferSize());
}

} // namespace monitor
} // namespace isearch
