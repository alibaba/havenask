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

#include "aios/network/anet/metric/KMonitorANetMetricReporter.h"
#include "aios/network/arpc/arpc/metric/KMonitorANetMetricReporterConfig.h"
#include "aios/network/arpc/arpc/metric/KMonitorClientMetricReporter.h"
#include "autil/Log.h"

namespace arpc {

class KMonitorANetClientMetricReporter : public ClientMetricReporter {
public:
    KMonitorANetClientMetricReporter(const KMonitorANetMetricReporterConfig &config);
    ~KMonitorANetClientMetricReporter() override = default;

public:
    void doReportMetric(ClientRPCStats *stats) override;
    bool init(anet::Transport *transport);

private:
    KMonitorANetMetricReporterConfig _config;
    std::shared_ptr<KMonitorClientMetricReporter> _arpcReporter;
    std::shared_ptr<anet::KMonitorANetMetricReporter> _anetReporter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace arpc