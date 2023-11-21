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
#include "indexlib/legacy/config/OfflineMergeConfig.h"

namespace indexlib::legacy::config {
AUTIL_LOG_SETUP(indexlib.config, OfflineMergeConfig);

OfflineMergeConfig::OfflineMergeConfig() : mergeParallelNum(0) {}

OfflineMergeConfig::~OfflineMergeConfig() {};

void OfflineMergeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("merge_config", mergeConfig, mergeConfig);
    json.Jsonize("plugin_config", pluginConfig, pluginConfig);
    json.Jsonize("period", periodMergeDescription, periodMergeDescription);
    json.Jsonize("merge_parallel_num", mergeParallelNum, mergeParallelNum);
    if (json.GetMode() == FROM_JSON) {
        const auto& jsonMap = json.GetMap();
        auto iter = jsonMap.find("doc_reclaim_source");
        if (iter != jsonMap.end()) {
            reclaimConfigs["doc_reclaim_source"] = iter->second;
        }
    }
}

bool OfflineMergeConfig::NeedReclaimTask() const
{
    return (reclaimConfigs.find("doc_reclaim_source") != reclaimConfigs.end()) ? true : false;
}

} // namespace indexlib::legacy::config
