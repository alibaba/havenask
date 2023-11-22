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
#include "indexlib/file_system/file/IntegratedCompressBlockDataRetriever.h"

#include "autil/ConstString.h"
#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, IntegratedCompressBlockDataRetriever);

IntegratedCompressBlockDataRetriever::IntegratedCompressBlockDataRetriever(
    const ReadOption& option, const std::shared_ptr<CompressFileInfo>& compressInfo,
    CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader, autil::mem_pool::Pool* pool)
    : CompressBlockDataRetriever(option, compressInfo, compressAddrMapper, dataFileReader, pool)
    , _baseAddress((char*)dataFileReader->GetBaseAddress())
{
    assert(_baseAddress != nullptr);
}

IntegratedCompressBlockDataRetriever::~IntegratedCompressBlockDataRetriever() { ReleaseBuffer(); }

FSResult<uint8_t*> IntegratedCompressBlockDataRetriever::RetrieveBlockData(size_t fileOffset,
                                                                           size_t& blockDataBeginOffset,
                                                                           size_t& blockDataLength) noexcept
{
    if (!GetBlockMeta(fileOffset, blockDataBeginOffset, blockDataLength)) {
        return {FSEC_OK, nullptr};
    }

    size_t blockIdx = _compressAddrMapper->OffsetToBlockIdx(blockDataBeginOffset);
    size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(blockIdx);
    size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(blockIdx);
    util::CompressHintData* hintData = _compressAddrMapper->GetHintData(blockIdx);

    _compressor->Reset();

    {
        ScopedDecompressMetricReporter scopeReporter(_reporter, _readOption.trace);
        if (!_compressor->DecompressToOutBuffer(_baseAddress + compressBlockOffset, compressBlockSize, hintData,
                                                _blockSize)) {
            AUTIL_LOG(ERROR, "decompress file [%s] failed, offset [%lu], compress len [%lu]",
                      _dataFileReader->DebugString().c_str(), compressBlockOffset, compressBlockSize);
            return {FSEC_ERROR, nullptr};
        }
    }

    size_t len = _compressor->GetBufferOutLen();
    char* data = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, char, len);
    memcpy(data, _compressor->GetBufferOut(), len);
    _addrVec.push_back(StringView(data, len));
    return {FSEC_OK, (uint8_t*)data};
}

void IntegratedCompressBlockDataRetriever::Reset() noexcept { ReleaseBuffer(); }

void IntegratedCompressBlockDataRetriever::ReleaseBuffer() noexcept
{
    for (size_t i = 0; i < _addrVec.size(); ++i) {
        char* buffer = (char*)_addrVec[i].data();
        size_t num = _addrVec[i].size();
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, buffer, num);
    }
    _addrVec.clear();
}
}} // namespace indexlib::file_system
