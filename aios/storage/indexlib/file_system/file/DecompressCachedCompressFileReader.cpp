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
#include "indexlib/file_system/file/DecompressCachedCompressFileReader.h"

#include <assert.h>
#include <iosfwd>
#include <memory>
#include <string.h>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/DynamicBuf.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"
#include "indexlib/util/cache/BlockAccessCounter.h"
#include "indexlib/util/cache/BlockAllocator.h"
#include "indexlib/util/cache/BlockCache.h"

namespace autil { namespace mem_pool {
class Pool;
}} // namespace autil::mem_pool
namespace indexlib { namespace file_system {
class Directory;
}} // namespace indexlib::file_system

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, DecompressCachedCompressFileReader);

bool DecompressCachedCompressFileReader::Init(const std::shared_ptr<FileReader>& fileReader,
                                              const std::shared_ptr<FileReader>& metaReader,
                                              const std::shared_ptr<CompressFileInfo>& compressInfo,
                                              IDirectory* directory) noexcept(false)
{
    if (!CompressFileReader::Init(fileReader, metaReader, compressInfo, directory)) {
        return false;
    }

    _blockFileNode = std::dynamic_pointer_cast<BlockFileNode>(fileReader->GetFileNode());
    assert(_blockFileNode);
    assert(_blockFileNode->EnableCacheDecompressFile());
    _fileId = _blockFileNode->GetCacheDecompressFileId();

    assert(_blockFileNode->GetBlockCache()->GetBlockSize() % compressInfo->blockSize == 0);
    _multipleNum = _blockFileNode->GetBlockCache()->GetBlockSize() / compressInfo->blockSize;
    _cachePriority =
        _blockFileNode->EnableHighPriority() ? autil::CacheBase::Priority::HIGH : autil::CacheBase::Priority::LOW;
    return true;
}

DecompressCachedCompressFileReader* DecompressCachedCompressFileReader::CreateSessionReader(Pool* pool) noexcept
{
    assert(_dataFileReader);
    assert(_compressAddrMapper);
    DecompressCachedCompressFileReader* cloneReader =
        IE_POOL_COMPATIBLE_NEW_CLASS(pool, DecompressCachedCompressFileReader, pool);
    cloneReader->DoInit(_dataFileReader, _metaFileReader, _compressAddrMapper, _compressInfo);
    cloneReader->_blockFileNode = _blockFileNode;
    cloneReader->_fileId = _fileId;
    cloneReader->_multipleNum = _multipleNum;
    cloneReader->_decompressMetricReporter = _decompressMetricReporter;
    return cloneReader;
}

void DecompressCachedCompressFileReader::LoadBuffer(size_t offset, ReadOption option) noexcept(false)
{
    assert(!InCurrentBlock(offset));
    assert(_dataFileReader);

    size_t compressBlockIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    size_t decompressBuffSize = _compressInfo->blockSize;
    size_t blockIdx = compressBlockIdx / _multipleNum;
    size_t beginCompressBlockIdx = blockIdx * _multipleNum;
    size_t endCompressBlockIdx = min(beginCompressBlockIdx + _multipleNum, _compressAddrMapper->GetBlockCount());

    autil::StringView toCopyData;
    blockid_t blockId(_fileId, blockIdx);
    BlockCache* blockCache = _blockFileNode->GetBlockCache();
    CacheBase::Handle* handle = NULL;
    Block* block = blockCache->Get(blockId, &handle);
    const BlockAllocatorPtr& blockAllocator = blockCache->GetBlockAllocator();
    if (NULL == block) {
        // block cache miss
        Block* block = blockAllocator->AllocBlock();
        block->id = blockId;
        char* addr = (char*)block->data;
        FreeBlockWhenException freeBlockWhenException(block, blockAllocator.get());
        // decompress multiple buffer & put in to cache
        for (size_t idx = beginCompressBlockIdx; idx < endCompressBlockIdx; idx++) {
            DecompressBuffer(idx, option);
            assert((addr + _compressors[0]->GetBufferOutLen() - (char*)block->data) <= blockCache->GetBlockSize());
            memcpy(addr, _compressors[0]->GetBufferOut(), _compressors[0]->GetBufferOutLen());
            if (idx == compressBlockIdx && (idx + 1) != endCompressBlockIdx) {
                toCopyData = autil::StringView(addr, _compressors[0]->GetBufferOutLen());
            }
            addr += _compressors[0]->GetBufferOutLen();
        }
        bool ret = blockCache->Put(block, &handle, _cachePriority);
        assert(ret);
        (void)ret;
        freeBlockWhenException.block = NULL; // normal return
    } else {
        // block cache hit
        size_t bufferLen = ((compressBlockIdx + 1) == _compressAddrMapper->GetBlockCount())
                               ? (GetUncompressedFileLength() - compressBlockIdx * decompressBuffSize)
                               : decompressBuffSize;
        assert(bufferLen <= decompressBuffSize);
        assert(block->id == blockId);
        char* addr = (char*)block->data + (compressBlockIdx - beginCompressBlockIdx) * decompressBuffSize;
        toCopyData = autil::StringView(addr, bufferLen);
    }
    if (!toCopyData.empty()) {
        // reset to target compress buffer
        _compressors[0]->Reset();
        DynamicBuf& outBuffer = _compressors[0]->GetOutBuffer();
        memcpy(outBuffer.getBuffer(), toCopyData.data(), toCopyData.size());
        outBuffer.movePtr(toCopyData.size());
    }
    blockCache->ReleaseHandle(handle);
    _curBlockIdxs[0] = compressBlockIdx;
}

future_lite::coro::Lazy<std::vector<ErrorCode>> DecompressCachedCompressFileReader::BatchLoadBuffer(
    const std::vector<std::pair<size_t, util::BufferCompressor*>>& blockInfo, ReadOption option) noexcept
{
    assert(_dataFileReader);
    vector<ErrorCode> result(blockInfo.size(), ErrorCode::FSEC_UNKNOWN);
    BatchIO batchIO;
    for (size_t i = 0; i < blockInfo.size(); ++i) {
        size_t compressBlockIdx = blockInfo[i].first;
        size_t decompressBuffSize = _compressInfo->blockSize;
        size_t blockIdx = compressBlockIdx / _multipleNum;
        size_t beginCompressBlockIdx = blockIdx * _multipleNum;
        size_t endCompressBlockIdx = min(beginCompressBlockIdx + _multipleNum, _compressAddrMapper->GetBlockCount());

        autil::StringView toCopyData;
        blockid_t blockId(_fileId, blockIdx);
        BlockCache* blockCache = _blockFileNode->GetBlockCache();
        CacheBase::Handle* handle = NULL;
        util::BufferCompressor* compressor = blockInfo[i].second;
        compressor->Reset();
        Block* block = blockCache->Get(blockId, &handle);
        if (NULL == block) {
            // block cache miss
            for (size_t idx = beginCompressBlockIdx; idx < endCompressBlockIdx; idx++) {
                size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(idx);
                size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(idx);
                uint8_t* buffer = nullptr;
                if (idx == beginCompressBlockIdx) {
                    buffer = reinterpret_cast<uint8_t*>(compressor->GetInBuffer().getBuffer());
                } else {
                    buffer = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, uint8_t, compressBlockSize);
                }
                batchIO.push_back(SingleIO(buffer, compressBlockSize, compressBlockOffset));
            }
        } else {
            // block cache hit
            size_t bufferLen = ((compressBlockIdx + 1) == _compressAddrMapper->GetBlockCount())
                                   ? (GetUncompressedFileLength() - compressBlockIdx * decompressBuffSize)
                                   : decompressBuffSize;
            assert(bufferLen <= decompressBuffSize);
            assert(block->id == blockId);
            char* addr = (char*)block->data + (compressBlockIdx - beginCompressBlockIdx) * decompressBuffSize;
            toCopyData = autil::StringView(addr, bufferLen);
            DynamicBuf& outBuffer = compressor->GetOutBuffer();
            memcpy(outBuffer.getBuffer(), toCopyData.data(), toCopyData.size());
            outBuffer.movePtr(toCopyData.size());
            blockCache->ReleaseHandle(handle);
            result[i] = ErrorCode::FSEC_OK;
        }
    }

    auto readResult = co_await _dataFileReader->BatchRead(batchIO, option);
    size_t batchIOIdx = 0;
    for (size_t i = 0; i < result.size(); ++i) {
        ScopedDecompressMetricReporter scopeReporter(_decompressMetricReporter, option.trace);
        if (result[i] != ErrorCode::FSEC_OK) {
            result[i] = ProcessSingleBlock(readResult, blockInfo[i].first, batchIO, blockInfo[i].second, batchIOIdx);
        }
    }
    co_return result;
}

ErrorCode DecompressCachedCompressFileReader::ProcessSingleBlock(const vector<FSResult<size_t>>& readResult,
                                                                 size_t compressBlockIdx, const BatchIO& batchIO,
                                                                 util::BufferCompressor* compressor,
                                                                 size_t& batchIOIdx) noexcept(false)
{
    size_t blockIdx = compressBlockIdx / _multipleNum;
    size_t beginCompressBlockIdx = blockIdx * _multipleNum;
    size_t endCompressBlockIdx = min(beginCompressBlockIdx + _multipleNum, _compressAddrMapper->GetBlockCount());

    bool thisBlockSuccess = true;
    autil::StringView toCopyData;
    blockid_t blockId(_fileId, blockIdx);
    BlockCache* blockCache = _blockFileNode->GetBlockCache();
    const BlockAllocatorPtr& blockAllocator = blockCache->GetBlockAllocator();
    Block* block = blockAllocator->AllocBlock();
    block->id = blockId;
    char* addr = (char*)block->data;
    CacheBase::Handle* handle = NULL;
    ErrorCode ec = ErrorCode::FSEC_OK;
    for (size_t idx = beginCompressBlockIdx; idx < endCompressBlockIdx; idx++) {
        util::CompressHintData* hintData = _compressAddrMapper->GetHintData(idx);
        auto& singleIO = batchIO[batchIOIdx];
        compressor->Reset();
        if (thisBlockSuccess && readResult[batchIOIdx].OK() &&
            compressor->DecompressToOutBuffer(static_cast<const char*>(singleIO.buffer), singleIO.len, hintData,
                                              _compressInfo->blockSize)) {
            assert((addr + compressor->GetBufferOutLen() - (char*)block->data) <= blockCache->GetBlockSize());
            memcpy(addr, compressor->GetBufferOut(), compressor->GetBufferOutLen());
            if (idx == compressBlockIdx && (idx + 1) != endCompressBlockIdx) {
                toCopyData = autil::StringView(addr, compressor->GetBufferOutLen());
            }
            addr += compressor->GetBufferOutLen();
        } else {
            if (ec == ErrorCode::FSEC_OK) {
                if (readResult[batchIOIdx].OK()) {
                    ec = ErrorCode::FSEC_ERROR;
                } else {
                    ec = readResult[batchIOIdx].ec;
                }
            }
            AUTIL_LOG(ERROR, "process file [%s] offset [%lu] len [%lu] failed", _dataFileReader->DebugString().c_str(),
                      singleIO.offset, singleIO.len);
            thisBlockSuccess = false;
        }
        if (idx != beginCompressBlockIdx) {
            uint8_t* buffer = (uint8_t*)singleIO.buffer;
            IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, buffer, singleIO.len);
        }
        batchIOIdx++;
    }
    if (thisBlockSuccess) {
        bool ret = blockCache->Put(block, &handle, _cachePriority);
        assert(ret);
        (void)ret;
        if (!toCopyData.empty()) {
            // reset to target compress buffer
            compressor->Reset();
            DynamicBuf& outBuffer = compressor->GetOutBuffer();
            memcpy(outBuffer.getBuffer(), toCopyData.data(), toCopyData.size());
            outBuffer.movePtr(toCopyData.size());
        }
        blockCache->ReleaseHandle(handle);
        return ErrorCode::FSEC_OK;
    } else {
        blockAllocator->FreeBlock(block);
    }
    return ec;
}

void DecompressCachedCompressFileReader::LoadBufferFromMemory(size_t offset, uint8_t* buffer, uint32_t bufLen,
                                                              bool enableTrace) noexcept(false)
{
    assert(!InCurrentBlock(offset));
    assert(_dataFileReader);

    size_t blockIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    blockid_t blockId(_fileId, blockIdx);
    BlockCache* blockCache = _blockFileNode->GetBlockCache();
    size_t blockSize = blockCache->GetBlockSize();

    CacheBase::Handle* handle = NULL;
    Block* block = blockCache->Get(blockId, &handle);
    if (NULL == block) {
        // block cache miss
        DecompressBuffer(blockIdx, buffer, bufLen, enableTrace);
        PutInCache(blockCache, blockId, &handle);
    } else {
        // block cache hit
        size_t bufferLen = ((blockIdx + 1) == _compressAddrMapper->GetBlockCount())
                               ? (GetUncompressedFileLength() - blockIdx * blockSize)
                               : blockSize;
        assert(bufferLen <= blockSize);
        assert(block->id == blockId);
        DynamicBuf& outBuffer = _compressors[0]->GetOutBuffer();
        memcpy(outBuffer.getBuffer(), block->data, bufferLen);
        outBuffer.movePtr(bufferLen);
    }
    blockCache->ReleaseHandle(handle);
    _curBlockIdxs[0] = blockIdx;
}

void DecompressCachedCompressFileReader::DecompressBuffer(size_t blockIdx, ReadOption option) noexcept(false)
{
    size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);

    _compressors[0]->Reset();
    DynamicBuf& inBuffer = _compressors[0]->GetInBuffer();
    if (compressBlockSize !=
        _blockFileNode->Read(inBuffer.getBuffer(), compressBlockSize, compressBlockOffset, option).GetOrThrow()) {
        INDEXLIB_FATAL_ERROR(FileIO, "read compress file[%s] failed, offset [%lu], compress len [%lu]",
                             _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
        return;
    }

    util::CompressHintData* hintData = _compressAddrMapper->GetHintData(blockIdx);
    inBuffer.movePtr(compressBlockSize);

    ScopedDecompressMetricReporter scopeReporter(_decompressMetricReporter, option.trace);
    if (!_compressors[0]->Decompress(hintData, _compressInfo->blockSize)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file [%s] failed, offset [%lu], compress len [%lu]",
                             _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
        return;
    }
}

void DecompressCachedCompressFileReader::DecompressBuffer(size_t blockIdx, uint8_t* buffer, uint32_t bufLen,
                                                          bool enableTrace) noexcept(false)
{
    size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);

    _compressors[0]->Reset();
    DynamicBuf& inBuffer = _compressors[0]->GetInBuffer();

    if (compressBlockSize > bufLen) {
        INDEXLIB_FATAL_ERROR(FileIO,
                             "read buffer from compress file[%s] failed, "
                             "offset [%lu], compress len [%lu], bufLen [%u]",
                             _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize, bufLen);
    }
    memcpy(inBuffer.getBuffer(), buffer, compressBlockSize);
    util::CompressHintData* hintData = _compressAddrMapper->GetHintData(blockIdx);
    inBuffer.movePtr(compressBlockSize);

    ScopedDecompressMetricReporter scopeReporter(_decompressMetricReporter, enableTrace);
    if (!_compressors[0]->Decompress(hintData, _compressInfo->blockSize)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file [%s] failed, offset [%lu], compress len [%lu]",
                             _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
        return;
    }
}

void DecompressCachedCompressFileReader::PutInCache(BlockCache* blockCache, const blockid_t& blockId,
                                                    CacheBase::Handle** handle) noexcept(false)
{
    assert(blockCache);
    assert(_compressors[0]->GetBufferOutLen() <= blockCache->GetBlockSize());

    const BlockAllocatorPtr& blockAllocator = blockCache->GetBlockAllocator();
    Block* block = blockAllocator->AllocBlock();
    block->id = blockId;
    FreeBlockWhenException freeBlockWhenException(block, blockAllocator.get());
    memcpy(block->data, _compressors[0]->GetBufferOut(), _compressors[0]->GetBufferOutLen());
    bool ret = blockCache->Put(block, handle, _cachePriority);
    assert(ret);
    (void)ret;
    freeBlockWhenException.block = NULL; // normal return
}
}} // namespace indexlib::file_system
