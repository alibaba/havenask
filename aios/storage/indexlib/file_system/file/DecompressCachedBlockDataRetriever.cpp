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
#include "indexlib/file_system/file/DecompressCachedBlockDataRetriever.h"

#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, DecompressCachedBlockDataRetriever);

DecompressCachedBlockDataRetriever::DecompressCachedBlockDataRetriever(
    const ReadOption& option, const std::shared_ptr<CompressFileInfo>& compressInfo,
    CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader, autil::mem_pool::Pool* pool) noexcept
    : CompressBlockDataRetriever(option, compressInfo, compressAddrMapper, dataFileReader, pool)
    , _prefetcher(((BlockFileNode*)(dataFileReader->GetFileNode().get()))->GetAccessor(), option)
    , _blockCache(nullptr)
    , _fileId(0)
    , _multipleNum(1)
    , _cachePriority(autil::CacheBase::Priority::LOW)
{
    auto fileNode = dataFileReader->GetFileNode();
    assert(fileNode->GetType() == FSFT_BLOCK);

    BlockFileNode* blockFileNode = (BlockFileNode*)(fileNode.get());
    assert(blockFileNode->EnableCacheDecompressFile());
    _blockCache = blockFileNode->GetBlockCache();
    assert(_blockCache);
    assert((_blockCache->GetBlockSize() % _blockSize) == 0);
    _multipleNum = _blockCache->GetBlockSize() / _blockSize;
    _fileId = blockFileNode->GetCacheDecompressFileId();
    _cachePriority =
        blockFileNode->EnableHighPriority() ? autil::CacheBase::Priority::HIGH : autil::CacheBase::Priority::LOW;
}

DecompressCachedBlockDataRetriever::~DecompressCachedBlockDataRetriever() noexcept { ReleaseBlocks(); }

uint8_t* DecompressCachedBlockDataRetriever::RetrieveBlockData(size_t fileOffset, size_t& blockDataBeginOffset,
                                                               size_t& blockDataLength) noexcept(false)
{
    if (!GetBlockMeta(fileOffset, blockDataBeginOffset, blockDataLength)) {
        return nullptr;
    }

    size_t compressBlockIdx = _compressAddrMapper->OffsetToBlockIdx(blockDataBeginOffset);
    size_t blockIdx = compressBlockIdx / _multipleNum;
    size_t beginCompressBlockIdx = blockIdx * _multipleNum;
    size_t endCompressBlockIdx = min(beginCompressBlockIdx + _multipleNum, _compressAddrMapper->GetBlockCount());

    blockid_t blockId(_fileId, blockIdx);
    CacheBase::Handle* handle = NULL;
    Block* block = _blockCache->Get(blockId, &handle);
    if (block != nullptr) {
        _handles.push_back(handle);
        return (uint8_t*)block->data + (compressBlockIdx - beginCompressBlockIdx) * _blockSize;
    }

    const BlockAllocatorPtr& blockAllocator = _blockCache->GetBlockAllocator();
    block = blockAllocator->AllocBlock();
    block->id = blockId;
    char* addr = (char*)block->data;
    FreeBlockWhenException freeBlockWhenException(block, blockAllocator.get());

    for (size_t idx = beginCompressBlockIdx; idx < endCompressBlockIdx; idx++) {
        DecompressBlockData(idx);
        assert((addr + _compressor->GetBufferOutLen() - (char*)block->data) <= _blockCache->GetBlockSize());
        memcpy(addr, _compressor->GetBufferOut(), _compressor->GetBufferOutLen());
        addr += _compressor->GetBufferOutLen();
    }
    bool ret = _blockCache->Put(block, &handle, _cachePriority);
    assert(ret);
    (void)ret;
    freeBlockWhenException.block = NULL; // normal return
    _handles.push_back(handle);
    return (uint8_t*)block->data + (compressBlockIdx - beginCompressBlockIdx) * _blockSize;
}

future_lite::coro::Lazy<ErrorCode> DecompressCachedBlockDataRetriever::Prefetch(size_t fileOffset,
                                                                                size_t length) noexcept
{
    size_t beginCompressBlockIdx = _compressAddrMapper->OffsetToBlockIdx(fileOffset);
    size_t beginBlockIdx = beginCompressBlockIdx / _multipleNum;
    size_t endCompressBlockIdx = _compressAddrMapper->OffsetToBlockIdx(fileOffset + length - 1);
    size_t endBlockIdx = endCompressBlockIdx / _multipleNum;
    for (size_t idx = beginBlockIdx; idx <= endBlockIdx; ++idx) {
        blockid_t blockId(_fileId, idx);
        CacheBase::Handle* handle = NULL;
        Block* block = _blockCache->Get(blockId, &handle);
        if (block != nullptr) {
            _handles.push_back(handle);
        } else {
            size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(idx);
            size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(idx);
            auto ret = co_await _prefetcher.Prefetch(compressBlockOffset, compressBlockSize);
            if (ret != ErrorCode::FSEC_OK) {
                co_return ret;
            }
        }
    }
    co_return ErrorCode::FSEC_OK;
}

void DecompressCachedBlockDataRetriever::Reset() noexcept { ReleaseBlocks(); }

void DecompressCachedBlockDataRetriever::ReleaseBlocks() noexcept
{
    for (size_t i = 0; i < _handles.size(); ++i) {
        _blockCache->ReleaseHandle(_handles[i]);
    }
    _handles.clear();
}
}} // namespace indexlib::file_system
