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
#include "aios/network/anet/metric/KMonitorANetMetricReporter.h"

#include "kmonitor/client/KMonitorFactory.h"

namespace anet {

AUTIL_LOG_SETUP(anet, KMonitorANetMetricReporter);

KMonitorANetMetricReporter::~KMonitorANetMetricReporter() {
    // should release report thread before dtor
    release();
}

bool KMonitorANetMetricReporter::init(Transport *transport, kmonitor::KMonitor *kMonitor) {
    if (!_config.enableANetMetric) {
        return true;
    }

    _transport = transport;
    if (_transport == nullptr) {
        AUTIL_LOG(WARN, "init kmon anet metric reporter failed, null transport");
        return false;
    }

    if (kMonitor == nullptr) {
        _kMonitor = kmonitor::KMonitorFactory::GetKMonitor("anet_kmonitor");
    } else {
        _kMonitor = kMonitor;
    }

    if (_kMonitor == nullptr) {
        AUTIL_LOG(WARN, "init kmon anet metric reporter failed, get kmonitor failed");
        return false;
    }

    _kMonitor->SetServiceName("anet");
    if (!doRegisterMetrics()) {
        return false;
    }

    return ANetMetricReporter::init(transport);
}

bool KMonitorANetMetricReporter::doRegisterMetrics() {
#define REGISTER_METRIC(metric, metric_name)                                                                           \
    metric.reset(_kMonitor->RegisterMetric(#metric_name, kmonitor::GAUGE, _level));                                    \
    if (metric == nullptr) {                                                                                           \
        AUTIL_LOG(WARN, "init kmon anet metric reporter failed, register metric anet.%s failed", #metric_name);        \
        return false;                                                                                                  \
    }

    REGISTER_METRIC(_txBytesMetric, connection.tx_bytes);
    REGISTER_METRIC(_rxBytesMetric, connection.rx_bytes);
    REGISTER_METRIC(_packetPostedMetric, connection.packet_posted);
    REGISTER_METRIC(_packetHandledMetric, connection.packet_handled);
    REGISTER_METRIC(_packetTimeoutMetric, connection.packet_timeout);
    REGISTER_METRIC(_queueSizeMetric, connection.queue_size);
    REGISTER_METRIC(_writeCntMetric, connection.write_cnt);
    REGISTER_METRIC(_writeOnceBytesMetric, connection.write_once_bytes);
    REGISTER_METRIC(_readCntMetric, connection.read_cnt);
    REGISTER_METRIC(_readOnceBytesMetric, connection.read_once_bytes);

#undef REGISTER_METRIC
    return true;
}

void KMonitorANetMetricReporter::doReportConnStat(const ConnStat &stat) {
    auto transportName = _transport->getName();
    if (transportName.empty()) {
        transportName = "unknown";
    }
    kmonitor::MetricsTags tags("transport", transportName);
    tags.AddTag("peer_addr", stat.peerAddr.empty() ? "unknown" : stat.peerAddr);
    tags.AddTag("local_addr", stat.localAddr.empty() ? "unknown" : stat.localAddr);

    _txBytesMetric->Report(&tags, stat.totalTxBytes);
    _rxBytesMetric->Report(&tags, stat.totalRxBytes);
    _packetPostedMetric->Report(&tags, stat.packetPosted);
    _packetHandledMetric->Report(&tags, stat.packetHandled);
    _packetTimeoutMetric->Report(&tags, stat.packetTimeout);
    _queueSizeMetric->Report(&tags, stat.queueSize);
    _writeCntMetric->Report(&tags, stat.callWriteCount);
    _readCntMetric->Report(&tags, stat.callReadCount);
    if (stat.callWriteCount > 0) {
        _writeOnceBytesMetric->Report(&tags, double(stat.totalTxBytes) / double(stat.callWriteCount));
    }
    if (stat.callReadCount > 0) {
        _readOnceBytesMetric->Report(&tags, double(stat.totalRxBytes) / double(stat.callReadCount));
    }
}
} // namespace anet