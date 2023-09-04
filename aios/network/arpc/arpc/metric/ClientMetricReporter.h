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

#include <map>
#include <memory>
#include <string>

#include "aios/network/arpc/arpc/metric/MetricReporterConfig.h"
#include "autil/Log.h"

namespace arpc {

class ClientRPCStats;

class ClientMetricReporter : public std::enable_shared_from_this<ClientMetricReporter> {
public:
    ClientMetricReporter(const MetricReporterConfig &config);
    virtual ~ClientMetricReporter() = default;

public:
    std::shared_ptr<ClientRPCStats> makeRPCStats();
    void reportRPCStats(ClientRPCStats *stats);

protected:
    virtual void doReportMetric(ClientRPCStats *stats) = 0;

protected:
    bool _enable{false};
    MetricReporterConfig _config;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace arpc