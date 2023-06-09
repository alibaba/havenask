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
#include "indexlib/file_system/file/NormalCompressFileReader.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/DynamicBuf.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

namespace autil { namespace mem_pool {
class Pool;
}} // namespace autil::mem_pool

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, NormalCompressFileReader);

NormalCompressFileReader* NormalCompressFileReader::CreateSessionReader(Pool* pool) noexcept
{
    assert(_dataFileReader);
    assert(_compressAddrMapper);
    NormalCompressFileReader* cloneReader = IE_POOL_COMPATIBLE_NEW_CLASS(pool, NormalCompressFileReader, pool);
    cloneReader->DoInit(_dataFileReader, _metaFileReader, _compressAddrMapper, _compressInfo);
    cloneReader->_decompressMetricReporter = _decompressMetricReporter;
    return cloneReader;
}

void NormalCompressFileReader::LoadBuffer(size_t offset, ReadOption option) noexcept(false)
{
    assert(!InCurrentBlock(offset));
    assert(_dataFileReader);
    size_t blockIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);
    util::CompressHintData* hintData = _compressAddrMapper->GetHintData(blockIdx);

    assert(_compressors[0]);
    assert(_dataFileReader);
    DynamicBuf& inBuffer = _compressors[0]->GetInBuffer();
    if (compressBlockSize !=
        _dataFileReader->Read(inBuffer.getBuffer(), compressBlockSize, compressBlockOffset, option).GetOrThrow()) {
        INDEXLIB_FATAL_ERROR(FileIO, "read compress file[%s] failed, offset [%lu], compress len [%lu]",
                             _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
        return;
    }
    inBuffer.movePtr(compressBlockSize);
    ScopedDecompressMetricReporter scopeReporter(_decompressMetricReporter, option.trace);
    if (!_compressors[0]->Decompress(hintData, _compressInfo->blockSize)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file [%s] failed, offset [%lu], compress len [%lu]",
                             _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
        return;
    }
    _curBlockIdxs[0] = blockIdx;
}

void NormalCompressFileReader::LoadBufferFromMemory(size_t offset, uint8_t* buffer, uint32_t bufLen,
                                                    bool enableTrace) noexcept(false)
{
    assert(!InCurrentBlock(offset));
    assert(_dataFileReader);
    size_t blockIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);
    util::CompressHintData* hintData = _compressAddrMapper->GetHintData(blockIdx);

    assert(_compressors[0]);
    assert(_dataFileReader);
    DynamicBuf& inBuffer = _compressors[0]->GetInBuffer();
    if (compressBlockSize > bufLen) {
        INDEXLIB_FATAL_ERROR(FileIO,
                             "read buffer from compress file[%s] failed, "
                             "offset [%lu], compress len [%lu], bufLen[%u]",
                             _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize, bufLen);
    }
    memcpy(inBuffer.getBuffer(), buffer, compressBlockSize);
    inBuffer.movePtr(compressBlockSize);
    ScopedDecompressMetricReporter scopeReporter(_decompressMetricReporter, enableTrace);
    if (!_compressors[0]->Decompress(hintData, _compressInfo->blockSize)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file [%s] failed, offset [%lu], compress len [%lu]",
                             _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
        return;
    }
    _curBlockIdxs[0] = blockIdx;
}

FL_LAZY(ErrorCode)
NormalCompressFileReader::LoadBufferAsyncCoro(size_t offset, ReadOption option) noexcept
{
    assert(!InCurrentBlock(offset));
    assert(_dataFileReader);
    size_t blockIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);
    util::CompressHintData* hintData = _compressAddrMapper->GetHintData(blockIdx);

    assert(_compressors[0]);
    assert(_dataFileReader);
    DynamicBuf& inBuffer = _compressors[0]->GetInBuffer();
    auto [ec, readLen] =
        FL_COAWAIT _dataFileReader->ReadAsyncCoro(inBuffer.getBuffer(), compressBlockSize, compressBlockOffset, option);
    FL_CORETURN_IF_FS_ERROR(ec, "ReadAsyncCoro failed");
    if (compressBlockSize != readLen) {
        AUTIL_LOG(ERROR, "read compress file[%s] failed, offset [%lu], compress len [%lu]",
                  _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
        FL_CORETURN FSEC_ERROR;
    }
    inBuffer.movePtr(compressBlockSize);
    ScopedDecompressMetricReporter scopeReporter(_decompressMetricReporter, option.trace);
    if (!_compressors[0]->Decompress(hintData, _compressInfo->blockSize)) {
        AUTIL_LOG(ERROR, "decompress file [%s] failed, offset [%lu], compress len [%lu]",
                  _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
        FL_CORETURN FSEC_ERROR;
    }
    _curBlockIdxs[0] = blockIdx;
    FL_CORETURN FSEC_OK;
}

future_lite::coro::Lazy<std::vector<ErrorCode>>
NormalCompressFileReader::BatchLoadBuffer(const std::vector<std::pair<size_t, util::BufferCompressor*>>& blockInfo,
                                          ReadOption option) noexcept
{
    vector<ErrorCode> result(blockInfo.size());
    BatchIO batchIO(blockInfo.size());
    assert(_dataFileReader);

    for (size_t i = 0; i < blockInfo.size(); ++i) {
        size_t blockIdx = blockInfo[i].first;
        size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
        size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);
        batchIO[i] = SingleIO(blockInfo[i].second->GetInBuffer().getBuffer(), compressBlockSize, compressBlockOffset);
    }
    auto readResult = co_await _dataFileReader->BatchRead(batchIO, option);
    assert(readResult.size() == batchIO.size());
    {
        ScopedDecompressMetricReporter scopeReporter(_decompressMetricReporter, option.trace);
        for (size_t i = 0; i < batchIO.size(); ++i) {
            auto& singleIO = batchIO[i];
            if (!readResult[i].OK()) {
                result[i] = readResult[i].ec;
                continue;
            }
            size_t blockIdx = blockInfo[i].first;
            util::CompressHintData* hintData = _compressAddrMapper->GetHintData(blockIdx);
            blockInfo[i].second->GetInBuffer().movePtr(singleIO.len);
            result[i] = blockInfo[i].second->Decompress(hintData, _compressInfo->blockSize) ? ErrorCode::FSEC_OK
                                                                                            : ErrorCode::FSEC_ERROR;
        }
    }
    co_return result;
}

}} // namespace indexlib::file_system
