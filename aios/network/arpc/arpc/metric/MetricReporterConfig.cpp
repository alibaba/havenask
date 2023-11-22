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
#include "aios/network/arpc/arpc/metric/MetricReporterConfig.h"

#include "autil/EnvUtil.h"

namespace arpc {

const std::string kReportArpcMetricsEnvName = "ARPC_METRIC_ENABLE";
const std::string kShowSlowRequestLogEnvName = "ARPC_SLOW_REQUEST_LOG";
const std::string kSlowRequestThresholdInUsEnvName = "ARPC_SLOW_REQUEST_THRESHOLD_IN_US";

MetricReporterConfig::MetricReporterConfig(bool enable) : enableArpcMetric(enable) 
{
    enableArpcMetric = autil::EnvUtil::getEnv(kReportArpcMetricsEnvName, enableArpcMetric);
    enableSlowRequestLog = autil::EnvUtil::getEnv(kShowSlowRequestLogEnvName, enableSlowRequestLog);
    slowRequestThresholdInUs = autil::EnvUtil::getEnv(kSlowRequestThresholdInUsEnvName, slowRequestThresholdInUs);
}

void MetricReporterConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("enable_arpc_metric", enableArpcMetric, enableArpcMetric);
    json.Jsonize("enable_slow_request_log", enableSlowRequestLog, enableSlowRequestLog);
    json.Jsonize("slow_request_threshold_us", slowRequestThresholdInUs, slowRequestThresholdInUs);
    json.Jsonize("type", type, type);
}

} // namespace arpc