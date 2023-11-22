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

#include "aios/network/arpc/arpc/metric/ServerMetricReporter.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/core/MutableMetric.h"

namespace arpc {

class KMonitorServerMetricReporter : public ServerMetricReporter {
public:
    KMonitorServerMetricReporter(const MetricReporterConfig &config, kmonitor::MetricLevel level);
    virtual ~KMonitorServerMetricReporter() = default;

public:
    bool init();
    void doReportMetric(ServerRPCStats *stats) override;

private:
    kmonitor::MetricLevel _level;
    kmonitor::MetricsTags _defaultTags;

    std::unique_ptr<kmonitor::MutableMetric> _serverQps;
    std::unique_ptr<kmonitor::MutableMetric> _errorQps;
    std::unique_ptr<kmonitor::MutableMetric> _serverTotalCost;
    std::unique_ptr<kmonitor::MutableMetric> _serverRequestUnpackCost;
    std::unique_ptr<kmonitor::MutableMetric> _serverRequestPendingCost;
    std::unique_ptr<kmonitor::MutableMetric> _serverRequestCallCost;
    std::unique_ptr<kmonitor::MutableMetric> _serverResponsePackCost;
    std::unique_ptr<kmonitor::MutableMetric> _serverResponseSendCost;
    std::unique_ptr<kmonitor::MutableMetric> _serverResponseIovSize;
    std::unique_ptr<kmonitor::MutableMetric> _serverThreadItemCount;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace arpc
