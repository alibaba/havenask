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

#include "autil/Log.h"
#include "autil/cache/cache.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/util/cache/Block.h"

namespace autil { namespace mem_pool {
class Pool;
}} // namespace autil::mem_pool
namespace indexlib {
namespace file_system {
class CompressFileInfo;
class Directory;
struct ReadOption;
} // namespace file_system
namespace util {
class BlockCache;
} // namespace util
} // namespace indexlib

namespace indexlib { namespace file_system {

class DecompressCachedCompressFileReader : public CompressFileReader
{
public:
    DecompressCachedCompressFileReader(autil::mem_pool::Pool* pool = NULL) noexcept
        : CompressFileReader(pool)
        , _fileId(0)
        , _multipleNum(1)
        , _cachePriority(autil::CacheBase::Priority::LOW)
    {
    }

    ~DecompressCachedCompressFileReader() noexcept {}

public:
    bool Init(const std::shared_ptr<FileReader>& fileReader, const std::shared_ptr<FileReader>& metaReader,
              const std::shared_ptr<CompressFileInfo>& compressInfo, IDirectory* directory) noexcept(false) override;

    DecompressCachedCompressFileReader* CreateSessionReader(autil::mem_pool::Pool* pool) noexcept override;

    std::string GetLoadTypeString() const noexcept override { return std::string("decompress_cached"); }

    FSResult<void> Close() noexcept override
    {
        auto ret = CompressFileReader::Close();
        _blockFileNode.reset();
        _fileId = 0;
        return ret;
    };

public:
    util::BlockCache* GetBlockCache() const noexcept { return _blockFileNode->GetBlockCache(); }

private:
    void LoadBuffer(size_t offset, ReadOption option) noexcept(false) override;
    // blockInfo: pair<blockIdx, BufferCompressor*>
    future_lite::coro::Lazy<std::vector<ErrorCode>>
    BatchLoadBuffer(const std::vector<std::pair<size_t, util::BufferCompressor*>>& blockInfo,
                    ReadOption option) noexcept override;
    ErrorCode ProcessSingleBlock(const std::vector<FSResult<size_t>>& readResult, size_t compressBlockIdx,
                                 const BatchIO& batchIO, util::BufferCompressor* compressor,
                                 size_t& batchIOIdx) noexcept(false);

    void DecompressBuffer(size_t blockIdx, ReadOption option) noexcept(false);
    void LoadBufferFromMemory(size_t offset, uint8_t* buffer, uint32_t bufLen,
                              bool enableTrace) noexcept(false) override;
    void DecompressBuffer(size_t blockIdx, uint8_t* buffer, uint32_t bufLen, bool enableTrace) noexcept(false);
    void PutInCache(util::BlockCache* blockCache, const util::blockid_t& blockId,
                    autil::CacheBase::Handle** handle) noexcept(false);

private:
    std::shared_ptr<BlockFileNode> _blockFileNode;
    uint64_t _fileId;
    size_t _multipleNum;
    autil::CacheBase::Priority _cachePriority;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<DecompressCachedCompressFileReader> DecompressCachedCompressFileReaderPtr;
}} // namespace indexlib::file_system
