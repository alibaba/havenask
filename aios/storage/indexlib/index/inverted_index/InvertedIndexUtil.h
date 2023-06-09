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

#include "autil/HashAlgorithm.h"
#include "indexlib/index/inverted_index/Types.h"
#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlib::index {

class InvertedIndexUtil
{
private:
    InvertedIndexUtil() {}
    ~InvertedIndexUtil() {}

public:
    static dictkey_t GetRetrievalHashKey(bool isNumberIndex, dictkey_t termHashKey)
    {
        if (!isNumberIndex) {
            return termHashKey;
        }

        // number index termHashKey may cause hash confict, rehash
        return autil::HashAlgorithm::hashString64((const char*)&termHashKey, sizeof(dictkey_t));
    }

    static bool IsNumberIndex(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfigPtr)
    {
        InvertedIndexType indexType = indexConfigPtr->GetInvertedIndexType();
        switch (indexType) {
            // number index with origin key may cause postingTable confict
        case it_number:
        case it_number_int8:
        case it_number_uint8:
        case it_number_int16:
        case it_number_uint16:
        case it_number_int32:
        case it_number_uint32:
        case it_number_int64:
        case it_number_uint64:
        case it_range:
        case it_date:
            return true;

        default:
            return false;
        }
        return false;
    }
    // Assume IST_IS_SHARDING type won't be used directly.
    // IST_NEED_SHARDING + shard id will be used instead.
    static std::string GetIndexName(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                    size_t shardId = 0)
    {
        // indexlibv2::config::InvertedIndexConfig::IndexShardingType shardingType = indexConfig->GetShardingType();
        // assert(shardingType != indexlibv2::config::InvertedIndexConfig::IST_IS_SHARDING);
        // if (shardingType == indexlibv2::config::InvertedIndexConfig::IST_NO_SHARDING) {
        //     assert(shardId == 0);
        //     return indexConfig->GetIndexName();
        // }
        // const std::vector<std::shared_ptr<indexlibv2::config::InvertedIndexConfig>>& indexConfigs =
        // indexConfig->GetShardingIndexConfigs(); assert(shardId < indexConfigs.size()); return
        // indexConfigs[shardId]->GetIndexName();
        return indexConfig->GetIndexName();
    }
};

} // namespace indexlib::index
