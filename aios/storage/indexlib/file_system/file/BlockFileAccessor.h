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

#include <array>
#include <assert.h>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/cache/cache.h"
#include "future_lite/Common.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/file_system/fslib/FslibOption.h"
#include "indexlib/util/FutureExecutor.h"
#include "indexlib/util/cache/Block.h"
#include "indexlib/util/cache/BlockAllocator.h"
#include "indexlib/util/cache/BlockCache.h"
#include "indexlib/util/cache/BlockHandle.h"

namespace future_lite {
class Executor;
struct Unit;
} // namespace future_lite
namespace indexlib { namespace file_system {
class PackageOpenMeta;
class FileSystemMetricsReporter;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class BlockFileAccessor
{
private:
    template <typename T>
    using Future = future_lite::Future<T>;

private:
    struct ReadContext {
        uint8_t* curBuffer = nullptr;
        size_t leftLen = 0;
        size_t curOffset = 0;

        ReadContext(uint8_t* buffer, size_t len, size_t offset) noexcept
            : curBuffer(buffer)
            , leftLen(len)
            , curOffset(offset)
        {
        }
    };

    using ReadContextPtr = std::shared_ptr<ReadContext>;

public:
    struct FileBlockMeta {
        size_t blockOffset;
        size_t inBlockOffset;
        size_t blockDataSize;

        FileBlockMeta(size_t _blockOffset, size_t _inBlockOffset, size_t _blockDataSize) noexcept
            : blockOffset(_blockOffset)
            , inBlockOffset(_inBlockOffset)
            , blockDataSize(_blockDataSize)
        {
        }
        FileBlockMeta() noexcept : blockOffset(0), inBlockOffset(0), blockDataSize(0) {}
    };

public:
    BlockFileAccessor(util::BlockCache* blockCache, bool useDirectIO, bool useHighPriority,
                      const std::string& linkRoot) noexcept
        : _linkRoot(linkRoot)
        , _blockCache(blockCache)
        , _fileId(0)
        , _fileLength(0)
        , _fileBeginOffset(0)
        , _blockSize(0)
        , _batchSize(DEFAULT_IO_BATCH_SIZE)
        , _cachePriority(useHighPriority ? autil::CacheBase::Priority::HIGH : autil::CacheBase::Priority::LOW)
        , _useDirectIO(useDirectIO)
        , TEST_mDisableCache(false)
        , _executor(util::FutureExecutor::GetInternalExecutor())
    {
        assert(_blockCache);
        if (unlikely(_blockCache->GetResourceInfo().maxMemoryUse == 0)) {
            TEST_mDisableCache = true;
        }
        _blockAllocatorPtr = blockCache->GetBlockAllocator();
        assert(_blockAllocatorPtr);
        _blockSize = _blockCache->GetBlockSize();
        _batchSize = _blockCache->GetIOBatchSize();
    }

    ~BlockFileAccessor() noexcept { [[maybe_unused]] auto ret = Close(); }

public:
    void InitTagMetricReporter(FileSystemMetricsReporter* reporter, const std::string& path) noexcept;

    FSResult<void> Open(const std::string& path, const PackageOpenMeta& packageOpenMeta) noexcept;

    FSResult<void> Open(const std::string& path, int64_t fileLength) noexcept;

    FSResult<void> Close() noexcept;

    uint64_t GetFileLength() const noexcept { return _fileLength; }
    util::BlockCache* GetBlockCache() const noexcept { return _blockCache; }

    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept;
    Future<size_t> ReadAsync(void* buffer, size_t length, size_t offset, ReadOption option) noexcept(false);
    FL_LAZY(FSResult<size_t>)
    ReadAsyncCoro(void* buffer, size_t length, size_t offset, ReadOption option) noexcept;

    future_lite::coro::Lazy<std::vector<FSResult<size_t>>> BatchReadOrdered(const BatchIO& batchIO,
                                                                            ReadOption option) noexcept;
    future_lite::coro::Lazy<std::vector<FSResult<util::BlockHandle>>>
    GetBlockHandles(const std::vector<size_t>& blockIdxs, ReadOption option) noexcept;
    future_lite::coro::Lazy<FSResult<size_t>> ReadFromFileWrapper(const std::vector<util::Block*>& blocks,
                                                                  size_t beginIdx, size_t endIdx, size_t offset,
                                                                  int advice, int64_t timeout) noexcept;

    FSResult<size_t> Prefetch(size_t length, size_t offset, ReadOption option) noexcept;
    future_lite::Future<size_t> PrefetchAsync(size_t length, size_t offset, ReadOption option) noexcept(false);
    FL_LAZY(FSResult<size_t>) PrefetchAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept;

    // TryRead return length if cache hit. return 0 if any cacheline miss
    size_t TryRead(void* buffer, size_t length, size_t offset) noexcept(false);
    std::vector<std::unique_ptr<util::BlockHandle>> TryGetBlockHandles(size_t offset, size_t length) noexcept(false);

    Future<util::BlockHandle> GetBlockAsync(size_t offset, ReadOption option) noexcept(false);
    FL_LAZY(FSResult<util::BlockHandle>) GetBlockAsyncCoro(size_t offset, ReadOption option) noexcept;
    bool GetBlock(size_t offset, util::BlockHandle& handle, ReadOption* option = nullptr) noexcept(false);
    bool GetBlockMeta(size_t offset, FileBlockMeta& meta) noexcept;

    size_t GetBlockCount(size_t offset, size_t length) const noexcept
    {
        size_t blockOffset = GetBlockOffset(offset);
        return ((length + offset - blockOffset) + _blockSize - 1) / _blockSize;
    }

    size_t GetBlockIdx(size_t offset) const noexcept { return (offset / _blockSize); }
    size_t GetInBlockOffset(size_t offset) const noexcept { return offset % _blockSize; }
    size_t GetBlockSize() const noexcept { return _blockSize; }

private:
    static const size_t DEFAULT_IO_BATCH_SIZE;

public:
    // for test
    void TEST_SetFile(FslibFileWrapperPtr file) noexcept { _filePtr = file; }

private:
    FSResult<size_t> DoRead(void* buffer, size_t length, size_t offset, const ReadOption& option) noexcept;
    Future<std::pair<util::Block*, autil::CacheBase::Handle*>>
    DoGetBlock(const util::blockid_t& blockID, uint64_t offset, ReadOption option) noexcept(false);
    FL_LAZY(FSResult<std::pair<util::Block*, autil::CacheBase::Handle*>>)
    DoGetBlockCoro(const util::blockid_t& blockID, uint64_t offset, ReadOption option) noexcept;

    Future<util::BlockHandle> DoGetBlock(size_t offset, ReadOption option) noexcept(false);
    FL_LAZY(FSResult<util::BlockHandle>) DoGetBlockCoro(size_t offset, ReadOption option) noexcept;

    Future<std::vector<util::BlockHandle>> DoGetBlocks(size_t blockIdx, size_t endBlockIdx,
                                                       std::vector<util::BlockHandle>&& handles,
                                                       ReadOption option) noexcept(false);
    Future<std::vector<util::BlockHandle>> DoGetBlocksCallback(size_t startMissBlock, size_t endMissBlock,
                                                               size_t endBlockIdx, util::Block* block,
                                                               autil::CacheBase::Handle* cacheHandle,
                                                               std::vector<util::BlockHandle>&& handles,
                                                               ReadOption option) noexcept(false);
    Future<std::vector<util::BlockHandle>> GetBlockHandles(size_t blockInFileIdx, size_t blockCount,
                                                           ReadOption option) noexcept(false);

    void FillBuffer(const util::BlockHandle& handle, ReadContext* ctx) noexcept;
    Future<future_lite::Unit> FillBuffer(size_t startBlockIdx, size_t cnt, const ReadContextPtr& ctx,
                                         ReadOption option) noexcept(false);

    Future<size_t> ReadFromBlock(const ReadContextPtr& ctx, ReadOption option) noexcept(false);
    FL_LAZY(FSResult<size_t>) ReadFromBlockCoro(const ReadContextPtr& ctx, ReadOption option) noexcept;

    Future<autil::CacheBase::Handle*> ReadBlockFromFileToCache(util::Block* block, uint64_t blockOffset,
                                                               ReadOption option) noexcept(false);
    size_t FillOneBlock(const SingleIO& io, util::Block* block, size_t blockId) const noexcept;
    future_lite::coro::Lazy<std::vector<FSResult<util::BlockHandle>>>
    BatchReadBlocksFromFile(const std::vector<size_t>& blockIds, ReadOption option) noexcept;
    void FillBatchIOFromHandles(const std::vector<FSResult<util::BlockHandle>>& blockHandles,
                                const std::vector<size_t>& blockIdxs, const BatchIO& batchIO,
                                std::vector<FSResult<size_t>>& result) const noexcept;

    void AddExecutorIfNotSet(ReadOption* option, future_lite::Executor* executor) noexcept
    {
        if (!option->executor && option->useInternalExecutor) {
            option->executor = executor;
        }
    }
    size_t GetBlockOffset(size_t offset) const noexcept { return offset - (offset % _blockSize); }

private:
    std::string _linkRoot;
    util::BlockCache* _blockCache;
    util::BlockAllocatorPtr _blockAllocatorPtr;
    FslibFileWrapperPtr _filePtr;
    uint64_t _fileId;
    uint64_t _fileLength;
    size_t _fileBeginOffset;
    uint32_t _blockSize;
    uint32_t _batchSize;
    autil::CacheBase::Priority _cachePriority;
    bool _useDirectIO;
    bool TEST_mDisableCache;
    util::BlockCache::TaggedMetricReporter _tagMetricReporter;

    future_lite::Executor* _executor;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BlockFileAccessor> BlockFileAccessorPtr;
///////////////////////////////////////////////////////////////////////

// to guarantee no memory leak when exception
struct FreeBlockWhenException {
    FreeBlockWhenException(util::Block* block_, util::BlockAllocator* allocator_) : allocator(allocator_), block(block_)
    {
    }
    ~FreeBlockWhenException()
    {
        if (unlikely(block != NULL)) {
            allocator->FreeBlock(block); // meeting a exception
        }
    }
    util::BlockAllocator* allocator;
    util::Block* block;

    FreeBlockWhenException(const FreeBlockWhenException&) = delete;
    FreeBlockWhenException& operator=(const FreeBlockWhenException&) = delete;

    FreeBlockWhenException(FreeBlockWhenException&& other) noexcept : allocator(other.allocator), block(other.block)
    {
        other.allocator = nullptr;
        other.block = nullptr;
    }
    FreeBlockWhenException& operator=(FreeBlockWhenException&& other) noexcept
    {
        std::swap(allocator, other.allocator);
        std::swap(block, other.block);
        return *this;
    }
};
}} // namespace indexlib::file_system
