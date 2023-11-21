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
#include "build_service/config/OfflineMergeConfig.h"

#include <cstdint>
#include <iosfwd>

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, OfflineMergeConfig);

OfflineMergeConfig::OfflineMergeConfig() : mergeParallelNum(0), mergeSleepTime(0), needWaitAlterField(true) {}

OfflineMergeConfig::~OfflineMergeConfig() {}

void OfflineMergeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("merge_config", mergeConfig, mergeConfig);
    json.Jsonize("period", periodMergeDescription, periodMergeDescription);
    json.Jsonize("merge_parallel_num", mergeParallelNum, mergeParallelNum);
    json.Jsonize("merge_sleep_time", mergeSleepTime, mergeSleepTime);
    json.Jsonize("plugin_config", mergePluginConfig, mergePluginConfig);
    json.Jsonize("need_wait_alter_field", needWaitAlterField, needWaitAlterField);
}

}} // namespace build_service::config
