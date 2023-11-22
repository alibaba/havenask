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
#include "build_service/config/SrcNodeConfig.h"

#include <iosfwd>
#include <limits>
#include <vector>

#include "alog/Logger.h"
#include "indexlib/config/load_config_list.h"
#include "indexlib/config/online_config.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, SrcNodeConfig);

SrcNodeConfig::SrcNodeConfig()
    : orderPreservingField()
    , blockCacheParam()
    , partitionMemoryInByte(std::numeric_limits<int64_t>::max())
    , exceptionRetryTime(RETRY_LIMIT)
    , enableOrderPreserving(false)
    , enableUpdateToAdd(false)
{
    options.SetIsOnline(true);
    options.GetOnlineConfig().SetNeedDeployIndex(false);
    options.GetOnlineConfig().SetNeedReadRemoteIndex(true);
    options.GetOnlineConfig().SetAllowReopenOnRecoverRt(false);
}

SrcNodeConfig::~SrcNodeConfig() {}

void SrcNodeConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("block_cache_param", blockCacheParam, blockCacheParam);
    json.Jsonize("partition_memory_in_byte", partitionMemoryInByte, partitionMemoryInByte);
    json.Jsonize("exception_retry_time", exceptionRetryTime, exceptionRetryTime);
    json.Jsonize("enable_order_preserving", enableOrderPreserving, enableOrderPreserving);
    json.Jsonize("enable_update_to_add", enableUpdateToAdd, enableUpdateToAdd);
    json.Jsonize("online_index_config", options.GetOnlineConfig(), options.GetOnlineConfig());
}

bool SrcNodeConfig::needSrcNode() const { return enableOrderPreserving || enableUpdateToAdd; }

bool SrcNodeConfig::validate() const
{
    if (exceptionRetryTime == 0) {
        BS_LOG(ERROR, "exception_retry_time should not be 0");
        return false;
    }
    if (!needSrcNode()) {
        return true;
    }
    for (auto& loadConfig : options.GetOnlineConfig().loadConfigList.GetLoadConfigs()) {
        if (loadConfig.NeedDeploy()) {
            BS_LOG(ERROR, "not allow deploy to local");
            return false;
        }
    }
    return true;
}
}} // namespace build_service::config
