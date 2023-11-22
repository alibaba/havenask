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

#include "autil/cache/cache.h"
#include "indexlib/file_system/file/BlockDataRetriever.h"
#include "indexlib/file_system/file/BlockPrefetcher.h"

namespace indexlib::util {
class BlockCache;
}

namespace indexlib { namespace file_system {
class BlockFileAccessor;

class NoCompressBlockDataRetriever final : public BlockDataRetriever
{
public:
    NoCompressBlockDataRetriever(const ReadOption& option, BlockFileAccessor* accessor);
    ~NoCompressBlockDataRetriever();

    NoCompressBlockDataRetriever(const NoCompressBlockDataRetriever&) = delete;
    NoCompressBlockDataRetriever& operator=(const NoCompressBlockDataRetriever&) = delete;
    NoCompressBlockDataRetriever(NoCompressBlockDataRetriever&&) = delete;
    NoCompressBlockDataRetriever& operator=(NoCompressBlockDataRetriever&&) = delete;

public:
    FSResult<uint8_t*> RetrieveBlockData(size_t fileOffset, size_t& blockDataBeginOffset,
                                         size_t& blockDataLength) noexcept override;
    future_lite::coro::Lazy<ErrorCode> Prefetch(size_t fileOffset, size_t length) noexcept override;
    void Reset() noexcept override;

private:
    void ReleaseBlocks() noexcept;

private:
    std::vector<util::BlockHandle> _handles;
    BlockPrefetcher _prefetcher;
    BlockFileAccessor* _accessor;
    ReadOption _readOption;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<NoCompressBlockDataRetriever> NoCompressBlockDataRetrieverPtr;
}} // namespace indexlib::file_system
