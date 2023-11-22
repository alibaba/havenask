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
#include "aios/network/arpc/arpc/metric/KMonitorANetServerMetricReporter.h"

namespace arpc {

AUTIL_LOG_SETUP(arpc, KMonitorANetServerMetricReporter);

KMonitorANetServerMetricReporter::KMonitorANetServerMetricReporter(const KMonitorANetMetricReporterConfig &config)
    : ServerMetricReporter(config.arpcConfig), _config{config} {}

bool KMonitorANetServerMetricReporter::init(anet::Transport *transport) {
    if (!_enable) {
        return true;
    }

    _arpcReporter = std::make_shared<KMonitorServerMetricReporter>(_config.arpcConfig, _config.metricLevel);
    if (_arpcReporter == nullptr || !_arpcReporter->init()) {
        AUTIL_LOG(WARN, "kmonitor ANet server metric reporter init failed, arpc reporter init failed");
        _enable = false;
        return false;
    }

    _anetReporter = std::make_shared<anet::KMonitorANetMetricReporter>(_config.anetConfig, _config.metricLevel);
    if (_anetReporter == nullptr || !_anetReporter->init(transport)) {
        AUTIL_LOG(WARN, "kmonitor ANet server metric reporter init failed, accl reporter init failed");
        _enable = false;
        return false;
    }
    AUTIL_LOG(INFO, "kmonitor ANet server metric reporter init done");
    return true;
}

void KMonitorANetServerMetricReporter::doReportMetric(ServerRPCStats *stats) {
    if (_arpcReporter != nullptr && stats != nullptr) {
        _arpcReporter->doReportMetric(stats);
    }
}

} // namespace arpc