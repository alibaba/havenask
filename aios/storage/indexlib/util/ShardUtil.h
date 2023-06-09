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

namespace indexlib { namespace util {

class ShardUtil
{
public:
    ShardUtil() {}
    ~ShardUtil() {}

public:
    static size_t TransformShardId(size_t globalShardId, size_t currentShardCount, size_t globalShardCount);

    static size_t GetShardId(uint64_t key, size_t shardCount);
};

///////////////////////////////////
inline size_t ShardUtil::TransformShardId(size_t globalShardId, size_t currentShardCount, size_t globalShardCount)
{
    assert(globalShardCount % currentShardCount == 0);
    assert((globalShardCount & (globalShardCount - 1)) == 0);
    assert((currentShardCount & (currentShardCount - 1)) == 0);
    return globalShardId & (currentShardCount - 1);
}

inline size_t ShardUtil::GetShardId(uint64_t key, size_t shardCount)
{
    assert((shardCount & (shardCount - 1)) == 0);
    assert(shardCount);
    return key & (shardCount - 1);
}
}} // namespace indexlib::util
