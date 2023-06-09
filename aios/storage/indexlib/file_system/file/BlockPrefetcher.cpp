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
#include "indexlib/file_system/file/BlockPrefetcher.h"

#include <algorithm>
#include <cstddef>

#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/util/cache/BlockCache.h"

namespace indexlib { namespace util {
struct Block;
}} // namespace indexlib::util

using namespace std;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BlockPrefetcher);

BlockPrefetcher::BlockPrefetcher(BlockFileAccessor* accessor, ReadOption option)
    : _blockCache(accessor->GetBlockCache())
    , _accessor(accessor)
    , _option(option)
{
    _option.advice = IO_ADVICE_LOW_LATENCY;
}

future_lite::coro::Lazy<ErrorCode> BlockPrefetcher::Prefetch(size_t offset, size_t len)
{
    vector<size_t> blockIds;
    for (size_t i = _accessor->GetBlockIdx(offset); i <= _accessor->GetBlockIdx(offset + len); ++i) {
        blockIds.push_back(i);
    }
    auto blockResult = co_await _accessor->GetBlockHandles(blockIds, _option);
    for (size_t i = 0; i < blockResult.size(); ++i) {
        if (blockResult[i].OK()) {
            _handles.push_back(move(blockResult[i].result));
        } else {
            co_return blockResult[i].ec;
        }
    }
    co_return FSEC_OK;
}

void BlockPrefetcher::Reset() { _handles.clear(); }

}} // namespace indexlib::file_system
