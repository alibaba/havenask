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
#include "aios/network/arpc/arpc/metric/ServerMetricReporter.h"

#include "aios/network/arpc/arpc/metric/ServerRPCStats.h"

namespace arpc {

AUTIL_LOG_SETUP(arpc, ServerMetricReporter);

ServerMetricReporter::ServerMetricReporter(const MetricReporterConfig &config) : _config(config) {
    _enable = _config.enableArpcMetric || _config.enableSlowRequestLog;
}

std::shared_ptr<ServerRPCStats> ServerMetricReporter::makeRPCStats() {
    if (!_enable) {
        return nullptr;
    }
    return std::make_shared<ServerRPCStats>(shared_from_this());
}

void ServerMetricReporter::reportRPCStats(ServerRPCStats *stats) {
    if (stats == nullptr || !_enable) {
        return;
    }

    doReportMetric(stats);

    if (_config.enableSlowRequestLog && _config.slowRequestThresholdInUs < stats->totalCostUs()) {
        AUTIL_LOG(WARN,
                  "slow server request. requestId: %d, totalCostUs: %ld, requestUnpack: %ld, requestPending: %ld, "
                  "requestCall: %ld, responsePack: %ld, responseSend: %ld",
                  stats->requestId(),
                  stats->totalCostUs(),
                  stats->requestUnpackCostUs(),
                  stats->requestPendingCostUs(),
                  stats->requestCallCostUs(),
                  stats->responsePackCostUs(),
                  stats->responseSendCostUs());
    }
}
} // namespace arpc