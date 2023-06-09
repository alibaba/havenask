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

#include <limits>
#include <stddef.h>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/cache/BlockHandle.h"

namespace indexlib {
namespace file_system {
class BlockFileAccessor;
} // namespace file_system
namespace util {
class BlockCache;
struct Block;
} // namespace util
} // namespace indexlib

namespace indexlib { namespace file_system {

class BlockPrefetcher
{
public:
    explicit BlockPrefetcher(BlockFileAccessor* accessor, ReadOption option);
    ~BlockPrefetcher() {}
    future_lite::coro::Lazy<ErrorCode> Prefetch(size_t offset, size_t length);
    void Reset();

private:
    util::BlockCache* _blockCache;
    BlockFileAccessor* _accessor;
    std::vector<util::BlockHandle> _handles;
    ReadOption _option;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BlockPrefetcher> BlockPrefetcherPtr;
}} // namespace indexlib::file_system
