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

#include "autil/ConstString.h"
#include "indexlib/base/Status.h"
#include "indexlib/common_define.h"
#include "indexlib/document/kv_document/kv_index_document.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/KeyHasherTyped.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(document, Document);

namespace indexlib { namespace index {

class ShardPartitioner
{
public:
    ShardPartitioner();
    virtual ~ShardPartitioner();

public:
    Status Init(const config::IndexPartitionSchemaPtr& schema, uint32_t shardCount);
    Status Init(HashFunctionType keyHasherType, uint32_t shardCount);
    Status Init(uint32_t shardCount);
    bool GetShardIdx(const autil::StringView& key, uint32_t& shardIdx)
    {
        assert(mKeyHasherType != hft_unknown);
        dictkey_t hashValue;
        if (!util::GetHashKey(mKeyHasherType, key, hashValue)) {
            IE_LOG(WARN, "get hash key[%s] failed", key.data());
            ERROR_COLLECTOR_LOG(WARN, "get hash key[%s] failed", key.data());
            return false;
        }
        shardIdx = hashValue & mShardMask;
        return true;
    }
    uint32_t GetShardCount() { return mShardCount; }

    bool GetShardIdx(document::KVIndexDocument* doc, uint32_t& shardIdx);
    void GetShardIdx(uint64_t keyHash, uint32_t& shardIdx);

private:
    virtual void InitKeyHasherType(const config::IndexPartitionSchemaPtr& schema) {};

protected:
    HashFunctionType mKeyHasherType;
    uint64_t mShardMask;
    bool mIsMultiRegion;
    uint32_t mShardCount;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShardPartitioner);
}} // namespace indexlib::index
