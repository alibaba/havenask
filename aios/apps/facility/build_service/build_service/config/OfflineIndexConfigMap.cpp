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
#include "build_service/config/OfflineIndexConfigMap.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, OfflineIndexConfigMap);

OfflineIndexConfigMap::OfflineIndexConfigMap() {}

OfflineIndexConfigMap::~OfflineIndexConfigMap() {}

void OfflineIndexConfigMap::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    OfflineIndexConfig& defaultOfflineConfig = _offlineIndexConfigMap[""];
    json.Jsonize("build_config", defaultOfflineConfig.buildConfig, defaultOfflineConfig.buildConfig);
    // json.Jsonize("merge_controller", defaultOfflineConfig.offlineMergeController,
    //              defaultOfflineConfig.offlineMergeController);
    json.Jsonize("merge_config", defaultOfflineConfig.offlineMergeConfig.mergeConfig,
                 defaultOfflineConfig.offlineMergeConfig.mergeConfig);
    typedef map<string, OfflineMergeConfig> MergeConfigMap;
    MergeConfigMap mergeConfigMap;

    if (json.GetMode() == FROM_JSON) {
        json.Jsonize("customized_merge_config", mergeConfigMap, mergeConfigMap);
        MergeConfigMap::iterator it = mergeConfigMap.begin();
        for (; it != mergeConfigMap.end(); ++it) {
            OfflineIndexConfig offlineIndexConfig = defaultOfflineConfig;
            offlineIndexConfig.offlineMergeConfig = it->second;
            _offlineIndexConfigMap[it->first] = offlineIndexConfig;
        }
    } else {
        InnerMap::iterator it = _offlineIndexConfigMap.begin();
        for (; it != _offlineIndexConfigMap.end(); ++it) {
            if (it->first != "") {
                mergeConfigMap[it->first] = it->second.offlineMergeConfig;
            }
        }
        json.Jsonize("customized_merge_config", mergeConfigMap);
    }
}
}} // namespace build_service::config
