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

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib::util {
class BlockAllocator;
class BlockCache;
} // namespace indexlib::util
namespace indexlib { namespace file_system {
class PackageOpenMeta;
struct ReadOption;
}} // namespace indexlib::file_system

namespace indexlib { namespace file_system {

class BlockFileNode final : public FileNode
{
public:
    BlockFileNode(util::BlockCache* blockCache, bool useDirectIO, bool cacheDecompressFile, bool useHighPriority,
                  const std::string& linkRoot) noexcept;
    ~BlockFileNode() noexcept;

public:
    FSFileType GetType() const noexcept override;
    size_t GetLength() const noexcept override;
    void* GetBaseAddress() const noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;

    future_lite::Future<size_t> ReadAsync(void* buffer, size_t length, size_t offset,
                                          ReadOption option) noexcept(false) override;
    FL_LAZY(FSResult<size_t>)
    ReadAsyncCoro(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    future_lite::coro::Lazy<std::vector<FSResult<size_t>>> BatchReadOrdered(const BatchIO& batchIO,
                                                                            ReadOption option) noexcept override;

    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    FSResult<void> Close() noexcept override;

    FSResult<size_t> Prefetch(size_t length, size_t offset, ReadOption option) noexcept override;
    future_lite::Future<size_t> PrefetchAsync(size_t length, size_t offset, ReadOption option) override;
    FL_LAZY(FSResult<size_t>) PrefetchAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept override;

    future_lite::Future<uint32_t> ReadUInt32Async(size_t offset, ReadOption option) noexcept(false) override;
    FL_LAZY(FSResult<uint32_t>) ReadUInt32AsyncCoro(size_t offset, ReadOption option) noexcept override;

    bool ReadOnly() const noexcept override { return true; }

    void InitMetricReporter(FileSystemMetricsReporter* reporter) noexcept override;

public:
    bool EnableHighPriority() const noexcept { return _useHighPriority; }
    bool EnableCacheDecompressFile() const noexcept { return _cacheDecompressFile; }
    uint64_t GetCacheDecompressFileId() const noexcept { return _cacheDecompressFileId; }
    util::BlockCache* GetBlockCache() const noexcept { return _accessor.GetBlockCache(); }
    BlockFileAccessor* GetAccessor() noexcept { return &_accessor; }

private:
    ErrorCode DoOpen(const std::string& path, FSOpenType openType, int64_t fileLength) noexcept override;
    ErrorCode DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept override;

    future_lite::Future<uint32_t> DoReadUInt32Async(size_t offset, size_t leftBytes, uint32_t currentValue,
                                                    ReadOption option) noexcept(false);
    FL_LAZY(FSResult<uint32_t>)
    DoReadUInt32AsyncCoro(size_t offset, size_t leftBytes, uint32_t currentValue, ReadOption option) noexcept;

private:
    BlockFileAccessor _accessor;
    bool _cacheDecompressFile;
    bool _useHighPriority;
    uint64_t _cacheDecompressFileId;

private:
    AUTIL_LOG_DECLARE();
    friend class BlockFileNodeTest;
};

typedef std::shared_ptr<BlockFileNode> BlockFileNodePtr;
}} // namespace indexlib::file_system
