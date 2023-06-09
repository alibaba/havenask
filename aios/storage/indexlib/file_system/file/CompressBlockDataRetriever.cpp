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
#include "indexlib/file_system/file/CompressBlockDataRetriever.h"

#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, CompressBlockDataRetriever);

CompressBlockDataRetriever::CompressBlockDataRetriever(const ReadOption& option,
                                                       const std::shared_ptr<CompressFileInfo>& compressInfo,
                                                       CompressFileAddressMapper* compressAddrMapper,
                                                       FileReader* dataFileReader, autil::mem_pool::Pool* pool)
    : BlockDataRetriever(compressInfo->blockSize, compressInfo->deCompressFileLen)
    , _compressAddrMapper(compressAddrMapper)
    , _dataFileReader(dataFileReader)
    , _pool(pool)
    , _readOption(option)
{
    assert(_compressAddrMapper != nullptr);
    assert(_dataFileReader != nullptr);
    _compressor =
        CompressFileReader::CreateCompressor(_pool, compressInfo, _compressAddrMapper->GetMaxCompressBlockSize());
    assert(_compressor);
}

CompressBlockDataRetriever::~CompressBlockDataRetriever() { IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _compressor); }

void CompressBlockDataRetriever::DecompressBlockData(size_t blockIdx) noexcept(false)
{
    size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);
    util::CompressHintData* hintData = _compressAddrMapper->GetHintData(blockIdx);

    _compressor->Reset();
    autil::DynamicBuf& inBuffer = _compressor->GetInBuffer();
    if (compressBlockSize !=
        _dataFileReader->Read(inBuffer.getBuffer(), compressBlockSize, compressBlockOffset, _readOption).GetOrThrow()) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file[%s] failed", _dataFileReader->DebugString().c_str());
    }
    inBuffer.movePtr(compressBlockSize);

    ScopedDecompressMetricReporter scopeReporter(_reporter, _readOption.trace);
    if (!_compressor->Decompress(hintData, _blockSize)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decompress file [%s] failed, offset [%lu], compress len [%lu]",
                             _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
    }
}
}} // namespace indexlib::file_system
