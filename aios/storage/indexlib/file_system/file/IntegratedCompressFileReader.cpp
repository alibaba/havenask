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
#include "indexlib/file_system/file/IntegratedCompressFileReader.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <string>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/file_system/file/CompressFileInfo.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

namespace autil { namespace mem_pool {
class Pool;
}} // namespace autil::mem_pool
namespace indexlib { namespace file_system {
class Directory;
}} // namespace indexlib::file_system

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, IntegratedCompressFileReader);

bool IntegratedCompressFileReader::Init(const std::shared_ptr<FileReader>& fileReader,
                                        const std::shared_ptr<FileReader>& metaReader,
                                        const std::shared_ptr<CompressFileInfo>& compressInfo,
                                        IDirectory* directory) noexcept(false)
{
    if (!CompressFileReader::Init(fileReader, metaReader, compressInfo, directory)) {
        return false;
    }

    _dataAddr = (char*)fileReader->GetBaseAddress();
    _batchSize = 1;
    return _dataAddr != NULL;
}

IntegratedCompressFileReader* IntegratedCompressFileReader::CreateSessionReader(Pool* pool) noexcept
{
    assert(_dataFileReader);
    assert(_compressAddrMapper);
    IntegratedCompressFileReader* cloneReader = IE_POOL_COMPATIBLE_NEW_CLASS(pool, IntegratedCompressFileReader, pool);
    cloneReader->DoInit(_dataFileReader, _metaFileReader, _compressAddrMapper, _compressInfo);
    cloneReader->_dataAddr = _dataAddr;
    cloneReader->_batchSize = 1;
    cloneReader->_decompressMetricReporter = _decompressMetricReporter;
    return cloneReader;
}

void IntegratedCompressFileReader::LoadBufferFromMemory(size_t offset, uint8_t* buffer, uint32_t bufLen,
                                                        bool enableTrace) noexcept(false)
{
    assert(false);
    INDEXLIB_FATAL_ERROR(UnSupported,
                         "IntegratedCompressFileReader [%s] not support load buffer from memory,"
                         " offset [%lu], bufLen [%u]",
                         _dataFileReader->DebugString().c_str(), offset, bufLen);
}

void IntegratedCompressFileReader::LoadBuffer(size_t offset, ReadOption option) noexcept(false)
{
    assert(!InCurrentBlock(offset));
    assert(_dataFileReader);
    size_t blockIdx = _compressAddrMapper->OffsetToBlockIdx(offset);
    ScopedDecompressMetricReporter scopeReporter(_decompressMetricReporter, option.trace);

    if (!DecompressOneBlock(blockIdx, _compressors[0])) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file [%s] failed, offset [%lu]",
                             _dataFileReader->DebugString().c_str(), offset);
    }
    _curBlockIdxs[0] = blockIdx;
}

future_lite::coro::Lazy<std::vector<ErrorCode>>
IntegratedCompressFileReader::BatchLoadBuffer(const std::vector<std::pair<size_t, util::BufferCompressor*>>& blockInfo,
                                              ReadOption option) noexcept
{
    ScopedDecompressMetricReporter scopeReporter(_decompressMetricReporter, option.trace);
    vector<ErrorCode> result(blockInfo.size());
    for (size_t i = 0; i < blockInfo.size(); ++i) {
        result[i] =
            DecompressOneBlock(blockInfo[i].first, blockInfo[i].second) ? ErrorCode::FSEC_OK : ErrorCode::FSEC_ERROR;
    }
    co_return result;
}

bool IntegratedCompressFileReader::DecompressOneBlock(size_t blockIdx, util::BufferCompressor* compressor) const
    noexcept(false)
{
    size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);
    util::CompressHintData* hintData = _compressAddrMapper->GetHintData(blockIdx);

    assert(compressor);
    if (!compressor->DecompressToOutBuffer(_dataAddr + compressBlockOffset, compressBlockSize, hintData,
                                           _compressInfo->blockSize)) {
        AUTIL_LOG(ERROR, "decompress file [%s] failed, offset [%lu], compress len [%lu]",
                  _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
        return false;
    }
    return true;
}

}} // namespace indexlib::file_system
