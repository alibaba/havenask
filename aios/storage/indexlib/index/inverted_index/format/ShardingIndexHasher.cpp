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
#include "indexlib/index/inverted_index/format/ShardingIndexHasher.h"

#include "indexlib/index/inverted_index/config/InvertedIndexConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, ShardingIndexHasher);

void ShardingIndexHasher::Init(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig)
{
    assert(indexConfig->GetShardingType() == indexlibv2::config::InvertedIndexConfig::IST_NEED_SHARDING);
    _dictHasher = IndexDictHasher(indexConfig->GetDictHashParams(), indexConfig->GetInvertedIndexType());
    _shardingCount = indexConfig->GetShardingIndexConfigs().size();
    _isNumberIndex = index::InvertedIndexUtil::IsNumberIndex(indexConfig);
}

} // namespace indexlib::index
