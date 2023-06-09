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
#include "build_service/config/ProcessorRuleConfig.h"

using namespace std;

namespace build_service { namespace config {

BS_LOG_SETUP(config, ProcessorRuleConfig);

void DetectSlowConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("src", src, src);
    json.Jsonize("condition", condition, condition);
    json.Jsonize("gap", gap, gap);
}

ProcessorRuleConfig::ProcessorRuleConfig()
    : partitionCount(1)
    , parallelNum(1)
    , incParallelNum(1)
    , incProcessorStartTs(-1)
{
}

ProcessorRuleConfig::~ProcessorRuleConfig() {}

void ProcessorRuleConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("partition_count", partitionCount, partitionCount);
    json.Jsonize("parallel_num", parallelNum, parallelNum);
    json.Jsonize("inc_parallel_num", incParallelNum, incParallelNum);
    json.Jsonize("inc_processor_start_timestamp", incProcessorStartTs, incProcessorStartTs);
    json.Jsonize("detect_slow_configs", detectSlowConfigs, detectSlowConfigs);
}

bool ProcessorRuleConfig::validate() const
{
    if (partitionCount == 0 || parallelNum == 0) {
        string errorMsg = "partition and parallelNum count can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

}} // namespace build_service::config
