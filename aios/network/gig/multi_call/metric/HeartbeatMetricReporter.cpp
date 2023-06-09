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
#include "aios/network/gig/multi_call/metric/HeartbeatMetricReporter.h"
#include <string>

using namespace std;
using namespace kmonitor;
namespace multi_call {
AUTIL_LOG_SETUP(multi_call, HeartbeatMetricReporter);

HeartbeatMetricReporter::HeartbeatMetricReporter(KMonitor *kMonitor)
    : _kMonitor(kMonitor) {

    std::map<std::string, std::string> tagkv;
    MetricsTagsPtr defaultTags = _kMonitor->GetMetricsTags(tagkv);

    DEFINE_METRIC(kMonitor, HeartbeatCostTime, "heartbeat.heartbeatCostTime",
                  GAUGE, NORMAL, defaultTags);
    DEFINE_METRIC(kMonitor, HeartbeatRequestLength,
                  "heartbeat.heartbeatRequestLength", GAUGE, NORMAL,
                  defaultTags);
    DEFINE_METRIC(kMonitor, HeartbeatRequestCount,
                  "heartbeat.heartbeatRequestCount", GAUGE, NORMAL,
                  defaultTags);

    DEFINE_MUTABLE_METRIC(kMonitor, HeartbeatQps, "heartbeat.heartbeatQps", QPS,
                          NORMAL);
    DEFINE_MUTABLE_METRIC(kMonitor, HeartbeatResponseLength,
                          "heartbeat.heartbeatResponseLength", GAUGE, NORMAL);
    DEFINE_MUTABLE_METRIC(kMonitor, HeartbeatResponseLatency,
                          "heartbeat.heartbeatResponseLatency", GAUGE, NORMAL);
}

HeartbeatMetricReporter::~HeartbeatMetricReporter() {}

kmonitor::MetricsTagsPtr
HeartbeatMetricReporter::getMetricsTags(const std::string &statusCode) {
    if (!_kMonitor) {
        return kmonitor::MetricsTagsPtr();
    }
    std::map<std::string, std::string> tagkv = { { "statusCode", statusCode } };
    return _kMonitor->GetMetricsTags(tagkv);
}

} // namespace multi_call
