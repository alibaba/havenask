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

#include <memory>

#include "indexlib/document/normal/Token.h"
#include "indexlib/index/common/DictHasher.h"
#include "indexlib/index/common/DictKeyInfo.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"

namespace indexlibv2::config {
class InvertedIndexConfig;
}

namespace indexlib::index {

class ShardingIndexHasher
{
public:
    ShardingIndexHasher() = default;
    ~ShardingIndexHasher() = default;

    void Init(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig);

    bool GetShardingIdx(const index::Term& term, index::DictKeyInfo& termHashKey, size_t& shardingIdx)
    {
        if (!_dictHasher.GetHashKey(term, termHashKey)) {
            AUTIL_LOG(ERROR, "invalid term [%s], index name [%s]", term.GetWord().c_str(), term.GetIndexName().c_str());
            return false;
        }
        shardingIdx = GetShardingIdx(termHashKey);
        return true;
    }

    size_t GetShardingIdx(const document::Token* token)
    {
        dictkey_t retrievalHashKey = InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, token->GetHashKey());
        return GetShardingIdxByRetrievalKey(retrievalHashKey, _shardingCount);
    }

    size_t GetShardingIdx(const index::DictKeyInfo& termHashKey)
    {
        if (termHashKey.IsNull()) {
            return GetShardingIdxForNullTerm();
        }
        dictkey_t retrievalHashKey = InvertedIndexUtil::GetRetrievalHashKey(_isNumberIndex, termHashKey.GetKey());
        return GetShardingIdxByRetrievalKey(retrievalHashKey, _shardingCount);
    }

    // null term will always be located to idx 0 shard
    size_t GetShardingIdxForNullTerm() const { return 0; }

    static size_t GetShardingIdxByRetrievalKey(dictkey_t retrievalHashKey, size_t shardingCount)
    {
        return retrievalHashKey % shardingCount;
    }

private:
    IndexDictHasher _dictHasher;
    size_t _shardingCount;
    bool _isNumberIndex;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
