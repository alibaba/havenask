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
#include "indexlib/file_system/file/BlockPrefetcher.h"
#include "indexlib/file_system/file/CompressBlockDataRetriever.h"

namespace indexlib::util {
class BlockCache;
}

namespace indexlib { namespace file_system {

class DecompressCachedBlockDataRetriever final : public CompressBlockDataRetriever
{
public:
    DecompressCachedBlockDataRetriever(const ReadOption& option, const std::shared_ptr<CompressFileInfo>& compressInfo,
                                       CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader,
                                       autil::mem_pool::Pool* pool) noexcept;

    ~DecompressCachedBlockDataRetriever() noexcept;

    DecompressCachedBlockDataRetriever(const DecompressCachedBlockDataRetriever&) = delete;
    DecompressCachedBlockDataRetriever& operator=(const DecompressCachedBlockDataRetriever&) = delete;
    DecompressCachedBlockDataRetriever(DecompressCachedBlockDataRetriever&&) = delete;
    DecompressCachedBlockDataRetriever& operator=(DecompressCachedBlockDataRetriever&&) = delete;

public:
    uint8_t* RetrieveBlockData(size_t fileOffset, size_t& blockDataBeginOffset,
                               size_t& blockDataLength) noexcept(false) override;
    void Reset() noexcept override;
    future_lite::coro::Lazy<ErrorCode> Prefetch(size_t fileOffset, size_t length) noexcept override;

private:
    void ReleaseBlocks() noexcept;

private:
    BlockPrefetcher _prefetcher;
    util::BlockCache* _blockCache;
    uint64_t _fileId;
    std::vector<autil::CacheBase::Handle*> _handles;
    size_t _multipleNum;
    autil::CacheBase::Priority _cachePriority;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DecompressCachedBlockDataRetriever> DecompressCachedBlockDataRetrieverPtr;
}} // namespace indexlib::file_system
