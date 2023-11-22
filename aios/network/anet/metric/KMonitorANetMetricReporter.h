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

#include "aios/network/anet/metric/ANetMetricReporter.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/core/MutableMetric.h"

namespace anet {

/*
 * KMonitorANetMetricReporter use to report anet metric to kmon
 * metrics get from anet connection connstat
 */
class KMonitorANetMetricReporter : public ANetMetricReporter {
public:
    KMonitorANetMetricReporter(const ANetMetricReporterConfig &config, kmonitor::MetricLevel level)
        : ANetMetricReporter(config), _level(level) {}
    virtual ~KMonitorANetMetricReporter();

public:
    bool init(Transport *transport) override { return init(transport, nullptr); }
    bool init(Transport *transport, kmonitor::KMonitor *kMonitor);

protected:
    bool doRegisterMetrics();
    void doReportConnStat(const ConnStat &stats) override;

private:
    kmonitor::KMonitor *_kMonitor{nullptr};
    kmonitor::MetricLevel _level;

    std::unique_ptr<kmonitor::MutableMetric> _rxBytesMetric;
    std::unique_ptr<kmonitor::MutableMetric> _txBytesMetric;
    std::unique_ptr<kmonitor::MutableMetric> _packetPostedMetric;
    std::unique_ptr<kmonitor::MutableMetric> _packetHandledMetric;
    std::unique_ptr<kmonitor::MutableMetric> _packetTimeoutMetric;
    std::unique_ptr<kmonitor::MutableMetric> _queueSizeMetric;
    std::unique_ptr<kmonitor::MutableMetric> _writeOnceBytesMetric;
    std::unique_ptr<kmonitor::MutableMetric> _writeCntMetric;
    std::unique_ptr<kmonitor::MutableMetric> _readOnceBytesMetric;
    std::unique_ptr<kmonitor::MutableMetric> _readCntMetric;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace anet