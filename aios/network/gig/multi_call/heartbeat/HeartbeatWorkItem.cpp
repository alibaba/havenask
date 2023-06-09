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
#include "aios/network/gig/multi_call/heartbeat/HeartbeatWorkItem.h"
#include "aios/network/gig/multi_call/heartbeat/HeartbeatClient.h"

namespace multi_call {

void HeartbeatWorkItem::process() {
    setResponseRecvTime();
    if (!_isFailed) {
        callback();
    }
    auto &heartbeatMetricReporter =
        _args->hbClient->getHeartbeatMetricReporterPtr();
    if (heartbeatMetricReporter) {
        heartbeatMetricReporter->reportHeartbeatRequestLength(
            _args->hbRequest->ByteSize());

        kmonitor::MetricsTagsPtr tags =
            heartbeatMetricReporter->getMetricsTags(_statusCode);
        heartbeatMetricReporter->reportHeartbeatQps(1, tags);
        heartbeatMetricReporter->reportHeartbeatResponseLength(
            _args->hbResponse->ByteSize(), tags);
        auto requestLatency = _responseRecvTime - _requestSendTime;
        heartbeatMetricReporter->reportHeartbeatResponseLatency(requestLatency,
                                                                tags);
    }
}

void HeartbeatWorkItem::callback() {
    if (_args != nullptr) {
        _args->hbClient->processHbResponse(_args->hbSpec, _args->providerVec,
                                           *(_args->hbResponse));
    }
}

} // namespace multi_call
