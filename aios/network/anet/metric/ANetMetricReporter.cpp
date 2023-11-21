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
#include "aios/network/anet/metric/ANetMetricReporter.h"

namespace anet {

AUTIL_LOG_SETUP(anet, ANetMetricReporter);

ANetMetricReporter::~ANetMetricReporter() { release(); }

bool ANetMetricReporter::init(Transport *transport) {
    if (!_config.enableANetMetric) {
        return true;
    }

    _transport = transport;
    if (_transport == nullptr) {
        AUTIL_LOG(WARN, "init anet metric reporter failed, null transport");
        return false;
    }

    auto name = _transport->getName() + "ANetMetric";
    _reportThread = autil::LoopThread::createLoopThread(
        std::bind(&ANetMetricReporter::reportThreadProc, this), _config.intervalMs * 1000, name);
    AUTIL_LOG(INFO,
              "init anet metric reporter done, report interval %ld, report thread %s",
              _config.intervalMs,
              name.c_str());
    return true;
}

void ANetMetricReporter::release() {
    if (_reportThread != nullptr) {
        _reportThread->stop();
        _reportThread.reset();
    }
}

void ANetMetricReporter::reportThreadProc() {
    std::vector<ConnStat> connStats;
    _transport->getTcpConnStats(connStats);

    std::map<std::string, ConnStat> statsMap;
    for (auto &connStat : connStats) {
        auto connName = connStat.localAddr + "|" + connStat.peerAddr;
        auto iter = _lastConnStats.find(connName);

        if (iter == _lastConnStats.end()) {
            doReportConnStat(connStat);
        } else {
            iter->second.totalRxBytes = connStat.totalRxBytes - iter->second.totalRxBytes;
            iter->second.totalTxBytes = connStat.totalTxBytes - iter->second.totalTxBytes;
            iter->second.packetPosted = connStat.packetPosted - iter->second.packetPosted;
            iter->second.packetHandled = connStat.packetHandled - iter->second.packetHandled;
            iter->second.packetTimeout = connStat.packetTimeout - iter->second.packetTimeout;
            iter->second.queueSize = connStat.queueSize;
            iter->second.callWriteCount = connStat.callWriteCount - iter->second.callWriteCount;
            iter->second.callReadCount = connStat.callReadCount - iter->second.callReadCount;
            doReportConnStat(iter->second);
        }
        statsMap[connName] = connStat;
    }

    _lastConnStats.swap(statsMap);
}

} // namespace anet