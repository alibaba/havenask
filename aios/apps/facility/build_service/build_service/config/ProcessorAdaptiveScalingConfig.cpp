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
#include "build_service/config/ProcessorAdaptiveScalingConfig.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, ProcessorAdaptiveScalingConfig);

void ProcessorAdaptiveScalingConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("max_count", maxCount);
    json.Jsonize("enable_adaptive_scaling", enableAdaptiveScaling, enableAdaptiveScaling);
    json.Jsonize("cpu_adjust_strategy", cpuStrategy, cpuStrategy);
    json.Jsonize("latency_adjust_strategy", latencyStrategy, latencyStrategy);
}

bool ProcessorAdaptiveScalingConfig::validate() const { return cpuStrategy.validate() && latencyStrategy.validate(); }

}} // namespace build_service::config
