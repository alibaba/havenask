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

#include "aios/network/anet/metric/ANetMetricReporterConfig.h"
#include "aios/network/arpc/arpc/metric/MetricReporterConfig.h"
#include "kmonitor/client/KMonitor.h"

namespace arpc {

class KMonitorANetMetricReporterConfig : public autil::legacy::Jsonizable
{
public:
    KMonitorANetMetricReporterConfig() { arpcConfig.type = "anet"; }
    ~KMonitorANetMetricReporterConfig() = default;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("arpc", arpcConfig);
        json.Jsonize("anet", anetConfig);
        json.Jsonize("metric_level", metricLevel);
    }

public:
    MetricReporterConfig arpcConfig {false};
    anet::ANetMetricReporterConfig anetConfig {false};
    kmonitor::MetricLevel metricLevel {kmonitor::NORMAL};
};

} // namespace arpc