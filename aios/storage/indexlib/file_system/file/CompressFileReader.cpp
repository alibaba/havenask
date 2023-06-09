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
#include "indexlib/file_system/file/CompressFileReader.h"

#include <iosfwd>
#include <stdint.h>
#include <string.h>

#include "autil/CommonMacros.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "future_lite/Common.h"
#include "future_lite/Helper.h"
#include "future_lite/Try.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/file/BlockByteSliceList.h"
#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"
#include "indexlib/util/buffer_compressor/BufferCompressorCreator.h"
#include "indexlib/util/cache/BlockAccessCounter.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::util;
namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, CompressFileReader);
size_t CompressFileReader::DEFAULT_BATCH_SIZE = []() {
    size_t batchSize = autil::EnvUtil::getEnv("INDEXLIB_COMPRESS_READ_BATCH_SIZE", 1);
    if (batchSize == 0) {
        batchSize = 1;
    }
    return batchSize;
}();

CompressFileReader::~CompressFileReader() noexcept
{
    for (size_t i = 0; i < _compressors.size(); ++i) {
        POOL_COMPATIBLE_DELETE_CLASS(_pool, _compressors[i]);
    }
    if (_prefetchBuf) {
        if (_pool) {
            size_t prefetchBufLen =
                std::max(_compressAddrMapper->GetMaxCompressBlockSize(), _compressAddrMapper->GetBlockSize());
            _pool->deallocate(_prefetchBuf, prefetchBufLen);
        } else {
            delete[] _prefetchBuf;
        }
        _prefetchBuf = nullptr;
    }
}

bool CompressFileReader::Init(const std::shared_ptr<FileReader>& fileReader,
                              const std::shared_ptr<FileReader>& metaReader,
                              const std::shared_ptr<CompressFileInfo>& compressInfo,
                              IDirectory* directory) noexcept(false)
{
    std::shared_ptr<CompressFileAddressMapper> mapper =
        LoadCompressAddressMapper(fileReader, metaReader, compressInfo, directory);
    if (!mapper) {
        return false;
    }
    assert(directory);
    return DoInit(fileReader, metaReader, mapper, compressInfo);
}

std::shared_ptr<CompressFileAddressMapper> CompressFileReader::LoadCompressAddressMapper(
    const std::shared_ptr<FileReader>& fileReader, const std::shared_ptr<FileReader>& metaReader,
    const std::shared_ptr<CompressFileInfo>& compressInfo, IDirectory* directory) noexcept(false)
{
    if (!fileReader) {
        return std::shared_ptr<CompressFileAddressMapper>();
    }

    bool useMetaFile = indexlib::util::GetTypeValueFromKeyValueMap(
        compressInfo->additionalInfo, COMPRESS_ENABLE_META_FILE, DEFAULT_COMPRESS_ENABLE_META_FILE);
    if (useMetaFile && !metaReader) {
        INDEXLIB_FATAL_ERROR(InconsistentState, "meta file should not be null.");
    }

    std::shared_ptr<FileReader> metaLoadReader = useMetaFile ? metaReader : fileReader;
    std::shared_ptr<CompressFileAddressMapper> mapper(new CompressFileAddressMapper);
    mapper->InitForRead(compressInfo, metaLoadReader, directory);
    return mapper;
}

FSResult<size_t> CompressFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    assert(_compressAddrMapper);
    if (offset >= GetUncompressedFileLength()) {
        return {FSEC_OK, 0};
    }
    _offset = offset;
    return Read(buffer, length, option);
}

future_lite::Future<uint32_t> CompressFileReader::ReadUInt32Async(size_t offset, ReadOption option) noexcept(false)
{
    auto bufferPtr = std::make_unique<uint32_t>(0);
    auto buffer = static_cast<void*>(bufferPtr.get());
    size_t bufferSize = std::min(sizeof(uint32_t), GetUncompressedFileLength() - offset);
    return ReadAsync(buffer, bufferSize, offset, option).thenValue([ptr = std::move(bufferPtr)](size_t readSize) {
        return *static_cast<uint32_t*>(ptr.get());
    });
}

future_lite::Future<uint32_t> CompressFileReader::ReadVUInt32Async(size_t offset, ReadOption option) noexcept(false)
{
    auto bufferPtr = std::make_unique<uint64_t>(0);
    auto buffer = static_cast<void*>(bufferPtr.get());
    size_t bufferSize = std::min(sizeof(uint64_t), GetUncompressedFileLength() - offset);
    return ReadAsync(buffer, bufferSize, offset, option).thenValue([ptr = std::move(bufferPtr)](size_t readSize) {
        uint8_t* byte = reinterpret_cast<uint8_t*>(ptr.get());
        uint32_t value = (*byte) & 0x7f;
        int shift = 7;
        while ((*byte) & 0x80) {
            ++byte;
            value |= ((*byte & 0x7F) << shift);
            shift += 7;
        }
        return value;
    });
}

bool CompressFileReader::CheckPrefetchHit(size_t offset,
                                          std::function<void(size_t, uint8_t*, uint32_t)> onHitCallback) noexcept(false)
{
    assert(_blockAccessor != nullptr);
    size_t blockIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    size_t compressBlockBeginOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);
    if (!_prefetchBuf) {
        size_t prefetchBufLen =
            std::max(_compressAddrMapper->GetMaxCompressBlockSize(), _compressAddrMapper->GetBlockSize());
        _prefetchBuf = (_pool) ? (uint8_t*)(_pool->allocate(prefetchBufLen)) : new uint8_t[prefetchBufLen];
    }
    if (compressBlockSize != _blockAccessor->TryRead(_prefetchBuf, compressBlockSize, compressBlockBeginOffset)) {
        return false;
    }
    onHitCallback(offset, _prefetchBuf, compressBlockSize);
    return true;
}

future_lite::Future<size_t> CompressFileReader::ReadAsync(void* buffer, size_t length, size_t offset,
                                                          ReadOption option) noexcept(false)
{
    assert(_compressAddrMapper);
    if (offset >= GetUncompressedFileLength()) {
        return future_lite::makeReadyFuture<size_t>(0);
    }
    _offset = offset;
    return PrefetchDataAsync(length, _offset, option)
        .thenValue([this, length, offset, buffer,
                    option](future_lite::Try<future_lite::Unit>&& unit) mutable -> future_lite::Future<size_t> {
            int64_t leftLen = length;
            uint8_t* cursor = (uint8_t*)buffer;
            try {
                while (true) {
                    if (!InCurrentBlock(_offset)) {
                        if (!_blockAccessor || option.executor == nullptr) {
                            ResetCompressorBuffer(0);
                            LoadBuffer(_offset, option);
                        } else {
                            auto onHitCallback = [this, enableTrace = option.trace](size_t offset, uint8_t* buffer,
                                                                                    uint32_t bufLen) {
                                ResetCompressorBuffer(0);
                                LoadBufferFromMemory(offset, buffer, bufLen, enableTrace);
                            };
                            if (!CheckPrefetchHit(_offset, std::move(onHitCallback))) {
                                INDEXLIB_FATAL_ERROR(
                                    FileIO,
                                    "Prefetch miss in CompressFileReader[%s], read length: [%lu], offset: [%lu]",
                                    _dataFileReader->GetLogicalPath().c_str(), length, offset);
                            }
                        }
                    }
                    assert(_compressors[0]);
                    size_t inBlockOffset = _compressAddrMapper->OffsetToInBlockOffset(_offset);
                    int64_t dataLeftInBlock = _compressors[0]->GetBufferOutLen() - inBlockOffset;
                    int64_t lengthToCopy = leftLen < dataLeftInBlock ? leftLen : dataLeftInBlock;

                    memcpy(cursor, _compressors[0]->GetBufferOut() + inBlockOffset, lengthToCopy);
                    cursor += lengthToCopy;
                    leftLen -= lengthToCopy;
                    _offset += lengthToCopy;

                    if (leftLen <= 0) {
                        assert(leftLen == 0);
                        return future_lite::makeReadyFuture<size_t>(length);
                    }

                    if (_offset >= GetUncompressedFileLength()) {
                        return future_lite::makeReadyFuture<size_t>(cursor - (uint8_t*)buffer);
                    }
                }
            } catch (...) {
                return future_lite::makeReadyFuture<size_t>(std::current_exception());
            }
            return future_lite::makeReadyFuture<size_t>(0);
        });
}

FL_LAZY(FSResult<size_t>)
CompressFileReader::ReadAsyncCoro(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    assert(_compressAddrMapper);
    if (offset >= GetUncompressedFileLength()) {
        FL_CORETURN FSResult<size_t> {FSEC_OK, 0};
    }
    _offset = offset;
    auto ec = FL_COAWAIT PrefetchDataAsyncCoro(length, _offset, option);
    FL_CORETURN2_IF_FS_ERROR(ec, 0ul, "PrefetchDataAsyncCoro failed, length[%lu], offset[%lu]", length, _offset);
    int64_t leftLen = length;
    char* cursor = (char*)buffer;
    while (true) {
        if (!InCurrentBlock(_offset)) {
            ResetCompressorBuffer(0);
            auto ec = FL_COAWAIT LoadBufferAsyncCoro(_offset, option);
            if (unlikely(ec != FSEC_OK)) {
                FL_CORETURN FSResult<size_t> {ec, (size_t)(cursor - (char*)buffer)};
            }
        }
        assert(_compressors[0]);
        size_t inBlockOffset = _compressAddrMapper->OffsetToInBlockOffset(_offset);
        int64_t dataLeftInBlock = _compressors[0]->GetBufferOutLen() - inBlockOffset;
        int64_t lengthToCopy = leftLen < dataLeftInBlock ? leftLen : dataLeftInBlock;

        memcpy(cursor, _compressors[0]->GetBufferOut() + inBlockOffset, lengthToCopy);
        cursor += lengthToCopy;
        leftLen -= lengthToCopy;
        _offset += lengthToCopy;

        if (leftLen <= 0) {
            assert(leftLen == 0);
            FL_CORETURN FSResult<size_t> {FSEC_OK, length};
        }

        if (_offset >= GetUncompressedFileLength()) {
            FL_CORETURN FSResult<size_t> {FSEC_OK, (size_t)(cursor - (char*)buffer)};
        }
    }
    FL_CORETURN FSResult<size_t> {FSEC_OK, 0};
}

// FL_LAZY(std::pair<void*, size_t>)
// CompressFileReader::ReadAsyncValue(size_t valueLen, size_t curInBlockOffset, size_t valueOffset, ReadOption& option,
//                                    autil::mem_pool::Pool* pool) noexcept(false)
// {

//     void* buffer = pool->allocate(valueLen);
//     assert(_compressors[0]);
//     if (valueLen <= _compressAddrMapper->GetBlockSize() - curInBlockOffset) {
//         // all value in first block
//         memcpy(buffer, _compressors[0]->GetBufferOut() + curInBlockOffset, valueLen);
//         FL_CORETURN std::make_pair<>(buffer, valueLen);
//     }
//     size_t readSize = _compressAddrMapper->GetBlockSize() - curInBlockOffset;
//     size_t leftReadSize = valueLen - readSize;
//     memcpy(buffer, _compressors[0]->GetBufferOut() + curInBlockOffset, readSize);
//     auto readLength =
//         FL_COAWAIT ReadAsyncCoro((uint8_t*)buffer + readSize, leftReadSize, valueOffset + readSize, option);
//     // TODO(xinfei.sxf) how to check has error
//     // if (unlikely(readLength.hasError() ||
//     //               readLength.value() != leftReadSize))
//     if (unlikely(readLength != leftReadSize)) {
//         INDEXLIB_FATAL_ERROR(IndexCollapsed, "read value failed readLength[%lu], leftReadSize[%lu]", readLength,
//                              leftReadSize);
//     }
//     FL_CORETURN make_pair<>(buffer, valueLen);
// }

FSResult<void> CompressFileReader::PrefetchData(size_t length, size_t offset, ReadOption option) noexcept
{
    size_t blockSize = _compressAddrMapper->GetBlockSize();
    size_t inBlockOffset = _compressAddrMapper->OffsetToInBlockOffset(offset);
    if (InCurrentBlock(offset)) {
        if (length <= blockSize - inBlockOffset) {
            return FSEC_OK;
        }
        offset += (blockSize - inBlockOffset);
        length -= (blockSize - inBlockOffset);
    }

    // [begin, end)
    size_t beginIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    size_t endIdx = _compressAddrMapper->OffsetToBlockIdx(offset + length + blockSize - 1);
    size_t beginOffset = _compressAddrMapper->CompressBlockAddress(beginIdx);
    size_t endOffset = _dataFileReader->GetLength();
    if (endIdx <= _compressAddrMapper->GetBlockCount()) {
        endOffset = _compressAddrMapper->CompressBlockAddress(endIdx);
    }
    if (beginOffset >= endOffset) {
        return FSEC_OK;
    }
    return _dataFileReader->Prefetch(endOffset - beginOffset, beginOffset, option).Code();
}

future_lite::Future<future_lite::Unit> CompressFileReader::PrefetchDataAsync(size_t length, size_t offset,
                                                                             ReadOption option) noexcept(false)
{
    size_t blockSize = _compressAddrMapper->GetBlockSize();
    size_t inBlockOffset = _compressAddrMapper->OffsetToInBlockOffset(offset);
    if (InCurrentBlock(offset)) {
        if (length <= blockSize - inBlockOffset) {
            return future_lite::makeReadyFuture(future_lite::Unit());
        }
        offset += (blockSize - inBlockOffset);
        length -= (blockSize - inBlockOffset);
    }

    // [begin, end)
    size_t beginIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    size_t endIdx = _compressAddrMapper->OffsetToBlockIdx(offset + length + blockSize - 1);
    size_t beginOffset = _compressAddrMapper->CompressBlockAddress(beginIdx);
    size_t endOffset = _dataFileReader->GetLength();
    if (endIdx <= _compressAddrMapper->GetBlockCount()) {
        endOffset = _compressAddrMapper->CompressBlockAddress(endIdx);
    }
    if (beginOffset >= endOffset) {
        return future_lite::makeReadyFuture(future_lite::Unit());
    }
    return _dataFileReader->PrefetchAsync(endOffset - beginOffset, beginOffset, option)
        .thenValue(
            [](future_lite::Try<size_t>&& readLen) { return future_lite::makeReadyFuture(future_lite::Unit()); });
}

FL_LAZY(FSResult<void>)
CompressFileReader::PrefetchDataAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept
{
    size_t blockSize = _compressAddrMapper->GetBlockSize();
    size_t inBlockOffset = _compressAddrMapper->OffsetToInBlockOffset(offset);
    if (InCurrentBlock(offset)) {
        if (length <= blockSize - inBlockOffset) {
            FL_CORETURN FSEC_OK;
        }
        offset += (blockSize - inBlockOffset);
        length -= (blockSize - inBlockOffset);
    }

    // [begin, end)
    size_t beginIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    size_t endIdx = _compressAddrMapper->OffsetToBlockIdx(offset + length + blockSize - 1);
    size_t beginOffset = _compressAddrMapper->CompressBlockAddress(beginIdx);
    size_t endOffset = _dataFileReader->GetLength();
    if (endIdx <= _compressAddrMapper->GetBlockCount()) {
        endOffset = _compressAddrMapper->CompressBlockAddress(endIdx);
    }
    if (beginOffset >= endOffset) {
        FL_CORETURN FSEC_OK;
    }
    auto [ec, readLen] = FL_COAWAIT _dataFileReader->PrefetchAsyncCoro(endOffset - beginOffset, beginOffset, option);
    if (FSEC_OPERATIONTIMEOUT == ec) {
        FL_CORETURN ec;
    }
    FL_CORETURN_IF_FS_ERROR(ec, "PrefetchAsyncCoro failed");
    (void)readLen;
    FL_CORETURN FSEC_OK;
}
future_lite::coro::Lazy<std::vector<FSResult<size_t>>> CompressFileReader::BatchReadOrdered(const BatchIO& batchIO,
                                                                                            ReadOption option) noexcept
{
    auto fillOneCompressor = [this](size_t bid, util::BufferCompressor* compressor, const SingleIO& single) mutable {
        size_t blockBegin = _compressAddrMapper->GetBlockSize() * bid;
        size_t inBlockOffset = max(single.offset, blockBegin) - blockBegin;
        size_t bufferOffset = 0;
        char* buffer = (char*)single.buffer;
        if (blockBegin > single.offset) {
            bufferOffset = blockBegin - single.offset;
        }
        return this->FillFromOneCompressor(inBlockOffset, single.len - bufferOffset, compressor, buffer + bufferOffset);
    };
    size_t batchSize = _batchSize;
    if (batchSize < _compressors.size()) {
        batchSize = _compressors.size();
    }
    std::vector<FSResult<size_t>> ret(batchIO.size(), {ErrorCode::FSEC_OK, 0});
    vector<size_t> lostBlocks;
    for (size_t i = 0; i < batchIO.size(); ++i) {
        const SingleIO& single = batchIO[i];
        pair<size_t, size_t> range = GetBlockRange(single.offset, single.len);
        for (size_t bid = range.first; bid < range.second; ++bid) {
            auto iter = _blockIdxToIndex.find(bid);
            if (iter == _blockIdxToIndex.end()) {
                lostBlocks.push_back(bid);
            } else {
                ret[i].result += fillOneCompressor(bid, _compressors[iter->second], single);
            }
        }
    }
    sort(lostBlocks.begin(), lostBlocks.end());
    lostBlocks.resize(distance(lostBlocks.begin(), unique(lostBlocks.begin(), lostBlocks.end())));
    if (!lostBlocks.empty()) {
        size_t idx = 0;
        size_t requiredCompressor = min(batchSize, lostBlocks.size());
        vector<pair<size_t, util::BufferCompressor*>> blockInfo(requiredCompressor);

        if (_compressors.size() < requiredCompressor) {
            AppendCompressorBuffer(requiredCompressor - _compressors.size());
        }
        while (idx < lostBlocks.size()) {
            for (size_t i = 0; i < batchSize; ++i) {
                if (idx == lostBlocks.size()) {
                    blockInfo.resize(i);
                    break;
                }
                size_t oldId = _curBlockIdxs[i];
                _curBlockIdxs[i] = _invalidBlockId;
                if (oldId != _invalidBlockId) {
                    _blockIdxToIndex.erase(_blockIdxToIndex.find(oldId));
                }
                ResetCompressorBuffer(i);
                blockInfo[i] = make_pair(lostBlocks[idx], _compressors[i]);
                idx++;
            }
            auto batchResult = co_await BatchLoadBuffer(blockInfo, option);
            for (size_t i = 0; i < batchResult.size(); ++i) {
                if (batchResult[i] == ErrorCode::FSEC_OK) {
                    _curBlockIdxs[i] = blockInfo[i].first;
                    _blockIdxToIndex[blockInfo[i].first] = i;
                }

                for (size_t ioIdx = 0; ioIdx < batchIO.size(); ++ioIdx) {
                    auto& single = batchIO[ioIdx];
                    pair<size_t, size_t> range = GetBlockRange(single.offset, single.len);
                    if (blockInfo[i].first >= range.first && blockInfo[i].first < range.second) {
                        if (batchResult[i] != ErrorCode::FSEC_OK) {
                            ret[ioIdx].ec = batchResult[i];
                        } else {
                            ret[ioIdx].result += fillOneCompressor(blockInfo[i].first, blockInfo[i].second, single);
                        }
                    }
                }
            }
        }
    }
    co_return ret;
}

// return value, [begin, end)
pair<size_t, size_t> CompressFileReader::GetBlockRange(size_t offset, size_t length) noexcept(false)
{
    pair<size_t, size_t> result = make_pair(0, 0);
    if (offset >= GetUncompressedFileLength() || length == 0) {
        return result;
    }
    size_t beginIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    size_t endIdx = _compressAddrMapper->OffsetToBlockIdx(offset + length - 1);
    ++endIdx;
    if (endIdx > _compressAddrMapper->GetBlockCount()) {
        endIdx = _compressAddrMapper->GetBlockCount();
    }
    result.first = beginIdx;
    result.second = endIdx;
    return result;
}

size_t CompressFileReader::FillFromOneCompressor(size_t inBlockOffset, size_t length, BufferCompressor* compressor,
                                                 void* buffer) const noexcept
{
    char* cursor = (char*)buffer;
    int64_t dataLeftInBlock = compressor->GetBufferOutLen() - inBlockOffset;
    assert(dataLeftInBlock > 0);
    int64_t lengthToCopy = length < dataLeftInBlock ? length : dataLeftInBlock;
    assert(lengthToCopy > 0);
    memcpy(cursor, compressor->GetBufferOut() + inBlockOffset, lengthToCopy);
    return lengthToCopy;
}

FSResult<size_t> CompressFileReader::Read(void* buffer, size_t length, ReadOption option) noexcept
{
    if (length == 0) {
        return {FSEC_OK, 0};
    }
    RETURN2_IF_FS_ERROR(PrefetchData(length, _offset, option).Code(), 0, "PrefetchData failed");

    int64_t leftLen = length;
    char* cursor = (char*)buffer;
    while (true) {
        if (!InCurrentBlock(_offset)) {
            ResetCompressorBuffer(0);
            RETURN2_IF_FS_EXCEPTION(LoadBuffer(_offset, option), (size_t)(cursor - (char*)buffer), "LoadBuffer failed");
        }

        assert(_compressors[0]);
        size_t copyInThisBlock = FillFromOneCompressor(_compressAddrMapper->OffsetToInBlockOffset(_offset), leftLen,
                                                       _compressors[0], cursor);
        leftLen -= copyInThisBlock;
        _offset += copyInThisBlock;
        cursor += copyInThisBlock;
        if (leftLen <= 0) {
            assert(leftLen == 0);
            return {FSEC_OK, length};
        }

        if (_offset >= GetUncompressedFileLength()) {
            return {FSEC_OK, (size_t)(cursor - (char*)buffer)};
        }
    }
    return {FSEC_OK, 0};
}

bool CompressFileReader::DoInit(const std::shared_ptr<FileReader>& fileReader,
                                const std::shared_ptr<FileReader>& metaReader,
                                const std::shared_ptr<CompressFileAddressMapper>& compressAddressMapper,
                                const std::shared_ptr<CompressFileInfo>& compressInfo) noexcept
{
    assert(fileReader);
    assert(compressAddressMapper);
    _dataFileReader = fileReader;
    _metaFileReader = metaReader;
    _compressAddrMapper = compressAddressMapper;
    _compressInfo = compressInfo;
    _offset = 0;
    _invalidBlockId = _compressAddrMapper->GetBlockCount();

    if (_dataFileReader->GetBaseAddress() == nullptr) {
        auto blockFileNode = std::dynamic_pointer_cast<BlockFileNode>(_dataFileReader->GetFileNode());
        if (blockFileNode != nullptr) {
            _blockAccessor = blockFileNode->GetAccessor();
        }
    }
    return true;
}

BufferCompressor* CompressFileReader::CreateCompressor(Pool* pool,
                                                       const std::shared_ptr<CompressFileInfo>& compressInfo,
                                                       size_t maxCompressedBlockSize) noexcept
{
    BufferCompressor* compressor = NULL;
    if (pool) {
        compressor = BufferCompressorCreator::CreateBufferCompressor(
            pool, compressInfo->compressorName, max(compressInfo->blockSize, maxCompressedBlockSize));
        if (!compressor) {
            AUTIL_LOG(ERROR, "create buffer compressor [%s] failed!", compressInfo->compressorName.c_str());
            return nullptr;
        }
    } else {
        compressor = BufferCompressorCreator::CreateBufferCompressor(compressInfo->compressorName);
        if (!compressor) {
            AUTIL_LOG(ERROR, "create buffer compressor [%s] failed!", compressInfo->compressorName.c_str());
            return nullptr;
        }
        compressor->SetBufferInLen(maxCompressedBlockSize);
        compressor->SetBufferOutLen(compressInfo->blockSize);
    }
    return compressor;
}

void CompressFileReader::AppendCompressorBuffer(size_t count) noexcept
{
    if (count == 0) {
        return;
    }
    for (size_t i = 0; i < count; ++i) {
        auto compressor = CreateCompressor(_pool, _compressInfo, _compressAddrMapper->GetMaxCompressBlockSize());
        assert(compressor);
        _compressors.push_back(compressor);
        _curBlockIdxs.push_back(_invalidBlockId);
    }
}

void CompressFileReader::ResetCompressorBuffer(size_t idx) noexcept
{
    if (_compressors.empty()) {
        AppendCompressorBuffer(1);
    } else {
        assert(idx < _compressors.size());
        _compressors[idx]->Reset();
    }
}

util::ByteSliceList* CompressFileReader::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    if (offset + length > GetUncompressedFileLength()) {
        AUTIL_LOG(ERROR,
                  "read file [%s] out of range, offset: [%lu], "
                  "read length: [%lu], uncompress file length: [%lu]",
                  DebugString().c_str(), offset, length, GetUncompressedFileLength());
        return nullptr;
    }

    BlockByteSliceList* sliceList =
        new BlockByteSliceList(option, _compressInfo, _compressAddrMapper.get(), _dataFileReader.get(), _pool);
    auto sliceLen = std::min(length, GetUncompressedFileLength() - offset);
    sliceList->InitMetricsReporter(_decompressMetricReporter);
    sliceList->AddBlock(offset, sliceLen);
    return sliceList;
}

string CompressFileReader::GetCompressFileName(const std::shared_ptr<FileReader>& reader,
                                               const IDirectory* directory) noexcept(false)
{
    string compressFileRelativePath;
    if (!PathUtil::GetRelativePath(directory->GetLogicalPath(), reader->GetLogicalPath(), compressFileRelativePath)) {
        INDEXLIB_FATAL_ERROR(InconsistentState,
                             "make compressFileRelativePath failed,"
                             " directory path [%s], file path [%s] ",
                             directory->DebugString().c_str(), reader->DebugString().c_str());
    }
    return compressFileRelativePath;
}

void CompressFileReader::InitDecompressMetricReporter(FileSystemMetricsReporter* reporter) noexcept
{
    if (!reporter || !reporter->SupportReportDecompressInfo()) {
        return;
    }
    map<string, string> tagMap;
    reporter->ExtractCompressTagInfo(_dataFileReader->GetLogicalPath(), _compressInfo->compressorName,
                                     _compressInfo->blockSize, _compressInfo->additionalInfo, tagMap);
    tagMap["load_type"] = GetLoadTypeString();
    _decompressMetricReporter.Init(reporter, tagMap);
}

}} // namespace indexlib::file_system
