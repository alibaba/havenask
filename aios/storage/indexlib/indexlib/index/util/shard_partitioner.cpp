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
#include "indexlib/index/util/shard_partitioner.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, ShardPartitioner);

ShardPartitioner::ShardPartitioner() : mKeyHasherType(hft_unknown), mShardMask(0), mIsMultiRegion(false), mShardCount(1)
{
}

ShardPartitioner::~ShardPartitioner() {}

Status ShardPartitioner::Init(HashFunctionType keyHasherType, uint32_t shardCount)
{
    mShardCount = shardCount;
    mKeyHasherType = keyHasherType;
    uint32_t priceNum = 1;
    while (priceNum < shardCount) {
        priceNum <<= 1;
    }

    if (priceNum != shardCount) {
        return Status::InvalidArgs("shardCount[%u] should be 2^n", shardCount);
    }
    mShardMask = shardCount - 1;
    return Status::OK();
}

Status ShardPartitioner::ShardPartitioner::Init(uint32_t shardCount)
{
    return ShardPartitioner::Init(hft_unknown, shardCount);
}

Status ShardPartitioner::Init(const IndexPartitionSchemaPtr& schema, uint32_t shardCount)
{
    assert(schema);
    assert(shardCount > 1);
    mShardCount = shardCount;
    if (schema->GetRegionCount() > 1) {
        mIsMultiRegion = true;
    } else {
        mIsMultiRegion = false;
        InitKeyHasherType(schema);
    }

    uint32_t priceNum = 1;
    while (priceNum < shardCount) {
        priceNum <<= 1;
    }

    if (priceNum != shardCount) {
        return Status::InvalidArgs("shardCount[%u] should be 2^n", shardCount);
    }
    mShardMask = shardCount - 1;
    return Status::OK();
}

bool ShardPartitioner::GetShardIdx(document::KVIndexDocument* document, uint32_t& shardIdx)
{
    assert(document);
    dictkey_t pkeyHash = document->GetPKeyHash();
    shardIdx = pkeyHash & mShardMask;
    return true;
}

void ShardPartitioner::GetShardIdx(uint64_t keyHash, uint32_t& shardIdx) { shardIdx = keyHash & mShardMask; }

}} // namespace indexlib::index
