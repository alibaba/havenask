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

#include "aios/network/arpc/arpc/metric/ClientMetricReporter.h"
#include "kmonitor/client/KMonitor.h"
#include "kmonitor/client/core/MutableMetric.h"

namespace arpc {

class KMonitorClientMetricReporter : public ClientMetricReporter
{
public:
    KMonitorClientMetricReporter(const MetricReporterConfig& config, kmonitor::MetricLevel level);
    virtual ~KMonitorClientMetricReporter() = default;

public:
    bool init();
    void doReportMetric(ClientRPCStats* stats) override;

private:
    kmonitor::MetricLevel _level;
    kmonitor::MetricsTags _defaultTags;

    std::unique_ptr<kmonitor::MutableMetric> _clientQps;
    std::unique_ptr<kmonitor::MutableMetric> _errorQps;

    std::unique_ptr<kmonitor::MutableMetric> _clientTotalCost;
    std::unique_ptr<kmonitor::MutableMetric> _clientRequestPendingCost;
    std::unique_ptr<kmonitor::MutableMetric> _clientRequestPackCost;
    std::unique_ptr<kmonitor::MutableMetric> _clientRequestSendCost;
    std::unique_ptr<kmonitor::MutableMetric> _clientRequestIovSize;
    std::unique_ptr<kmonitor::MutableMetric> _clientResponseWaitCost;
    std::unique_ptr<kmonitor::MutableMetric> _clientResponsePopCost;
    std::unique_ptr<kmonitor::MutableMetric> _clientResponseUnpackCost;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace arpc