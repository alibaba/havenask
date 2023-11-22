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
#include "indexlib/file_system/file/NormalCompressBlockDataRetriever.h"

#include "autil/ConstString.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/CompressFileAddressMapper.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/buffer_compressor/BufferCompressor.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, NormalCompressBlockDataRetriever);

NormalCompressBlockDataRetriever::NormalCompressBlockDataRetriever(
    const ReadOption& option, const std::shared_ptr<CompressFileInfo>& compressInfo,
    CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader, autil::mem_pool::Pool* pool) noexcept
    : CompressBlockDataRetriever(option, compressInfo, compressAddrMapper, dataFileReader, pool)
    , _prefetcher(((BlockFileNode*)(dataFileReader->GetFileNode().get()))->GetAccessor(), option)
{
}

NormalCompressBlockDataRetriever::~NormalCompressBlockDataRetriever() noexcept { ReleaseBuffer(); }

FSResult<uint8_t*> NormalCompressBlockDataRetriever::RetrieveBlockData(size_t fileOffset, size_t& blockDataBeginOffset,
                                                                       size_t& blockDataLength) noexcept
{
    if (!GetBlockMeta(fileOffset, blockDataBeginOffset, blockDataLength)) {
        return {FSEC_OK, nullptr};
    }

    size_t blockIdx = _compressAddrMapper->OffsetToBlockIdx(blockDataBeginOffset);
    RETURN2_IF_FS_ERROR(DecompressBlockData(blockIdx).Code(), nullptr, "decompress block failed");
    size_t len = _compressor->GetBufferOutLen();
    char* data = IE_POOL_COMPATIBLE_NEW_VECTOR(_pool, char, len);
    memcpy(data, _compressor->GetBufferOut(), len);
    _addrVec.push_back(StringView(data, len));
    return {FSEC_OK, (uint8_t*)data};
}

void NormalCompressBlockDataRetriever::Reset() noexcept
{
    ReleaseBuffer();
    _prefetcher.Reset();
}

void NormalCompressBlockDataRetriever::ReleaseBuffer() noexcept
{
    for (size_t i = 0; i < _addrVec.size(); ++i) {
        char* buffer = (char*)_addrVec[i].data();
        size_t num = _addrVec[i].size();
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_pool, buffer, num);
    }
    _addrVec.clear();
}
future_lite::coro::Lazy<ErrorCode> NormalCompressBlockDataRetriever::Prefetch(size_t fileOffset, size_t length) noexcept
{
    size_t beginCompressBlockIdx = _compressAddrMapper->OffsetToBlockIdx(fileOffset);
    size_t endCompressBlockIdx = _compressAddrMapper->OffsetToBlockIdx(fileOffset + length - 1);
    for (size_t idx = beginCompressBlockIdx; idx <= endCompressBlockIdx; ++idx) {
        size_t compressBlockOffset = _compressAddrMapper->CompressBlockAddress(idx);
        size_t compressBlockSize = _compressAddrMapper->CompressBlockLength(idx);
        auto ret = co_await _prefetcher.Prefetch(compressBlockOffset, compressBlockSize);
        if (ret != ErrorCode::FSEC_OK) {
            co_return ret;
        }
    }
    co_return ErrorCode::FSEC_OK;
}
}} // namespace indexlib::file_system
