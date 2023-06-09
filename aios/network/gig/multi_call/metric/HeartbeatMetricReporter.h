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
#ifndef MULTI_CALL_SERVICE_HEARTBEATMETRICREPORTER_H_
#define MULTI_CALL_SERVICE_HEARTBEATMETRICREPORTER_H_

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/util/MetricUtil.h"
#include "kmonitor/client/core/MutableMetric.h"

using namespace kmonitor;

namespace multi_call {

class HeartbeatMetricReporter {
public:
    explicit HeartbeatMetricReporter(kmonitor::KMonitor *kMonitor);
    ~HeartbeatMetricReporter();

    HeartbeatMetricReporter(const HeartbeatMetricReporter &) = delete;
    HeartbeatMetricReporter &
    operator=(const HeartbeatMetricReporter &) = delete;

    DECLARE_METRIC(HeartbeatCostTime);
    DECLARE_METRIC(HeartbeatRequestLength);
    DECLARE_METRIC(HeartbeatRequestCount);

    DECLARE_MUTABLE_METRIC(HeartbeatQps);
    DECLARE_MUTABLE_METRIC(HeartbeatResponseLength);
    DECLARE_MUTABLE_METRIC(HeartbeatResponseLatency);

public:
    kmonitor::MetricsTagsPtr getMetricsTags(const std::string &statusCode);

private:
    kmonitor::KMonitor *_kMonitor;

private:
    AUTIL_LOG_DECLARE();
};
MULTI_CALL_TYPEDEF_PTR(HeartbeatMetricReporter);

} // namespace multi_call

#endif // MULTI_CALL_SERVICE_HEARTBEATMETRICREPORTER_H_
