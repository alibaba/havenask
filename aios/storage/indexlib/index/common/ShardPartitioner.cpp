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
#include "indexlib/index/common/ShardPartitioner.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, ShardPartitioner);

ShardPartitioner::ShardPartitioner() {}

ShardPartitioner::~ShardPartitioner() {}

Status ShardPartitioner::Init(indexlib::HashFunctionType keyHasherType, uint32_t shardCount)
{
    _shardCount = shardCount;
    _keyHasherType = keyHasherType;
    uint32_t priceNum = 1;
    while (priceNum < shardCount) {
        priceNum <<= 1;
    }

    if (priceNum != shardCount) {
        return Status::InvalidArgs("shardCount[%u] should be 2^n", shardCount);
    }
    _shardMask = shardCount - 1;
    return Status::OK();
}

Status ShardPartitioner::Init(uint32_t shardCount) { return Init(indexlib::hft_unknown, shardCount); }

Status ShardPartitioner::Init(const std::shared_ptr<config::TabletSchema>& schema, uint32_t shardCount)
{
    assert(schema);
    assert(shardCount > 1);
    _shardCount = shardCount;
    _isMultiRegion = false;
    InitKeyHasherType(schema);

    uint32_t priceNum = 1;
    while (priceNum < shardCount) {
        priceNum <<= 1;
    }

    if (priceNum != shardCount) {
        return Status::InvalidArgs("shardCount[%u] should be 2^n", shardCount);
    }
    _shardMask = shardCount - 1;
    return Status::OK();
}

void ShardPartitioner::GetShardIdx(uint64_t keyHash, uint32_t& shardIdx) { shardIdx = keyHash & _shardMask; }

} // namespace indexlibv2::index
