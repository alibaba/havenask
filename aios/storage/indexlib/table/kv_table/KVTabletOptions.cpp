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
#include "indexlib/table/kv_table/KVTabletOptions.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTabletOptions);

uint32_t KVTabletOptions::GetShardNum() const
{
    uint32_t shardNum = 1;
    std::string path;
    if (_tabletOptions->IsOnline()) {
        path = "online_index_config.build_config.sharding_column_num";
    } else {
        path = "offline_index_config.build_config.sharding_column_num";
    }
    if (!_tabletOptions->GetFromRawJson(path, &shardNum).IsOK()) {
        return 1;
    }
    return shardNum;
}

uint32_t KVTabletOptions::GetLevelNum() const
{
    uint32_t levelNum = 2;
    std::string path;
    if (_tabletOptions->IsOnline()) {
        path = "online_index_config.build_config.level_num";
    } else {
        path = "offline_index_config.build_config.level_num";
    }
    if (!_tabletOptions->GetFromRawJson(path, &levelNum).IsOK()) {
        AUTIL_LOG(WARN, "kv table level num not existed, rewrite to 2");
        return 2;
    }
    if (levelNum <= 1) {
        AUTIL_LOG(WARN, "kv table level num less than 1, rewrite to 2");
        levelNum = 2;
    }
    return levelNum;
}

bool KVTabletOptions::IsMemoryReclaimEnabled() const
{
    if (!_tabletOptions->IsOnline()) {
        return false;
    }
    std::string path = "online_index_config.build_config.enable_kv_memory_reclaim";
    bool ret = false;
    if (!_tabletOptions->GetFromRawJson(path, &ret).IsOK()) {
        return false;
    }
    return ret;
}

} // namespace indexlibv2::table
