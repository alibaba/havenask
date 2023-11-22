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
#include "indexlib/file_system/file/BlockByteSliceList.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <stdint.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/file/BlockFileNode.h"
#include "indexlib/file_system/file/BlockPrefetcher.h"
#include "indexlib/file_system/file/DecompressCachedBlockDataRetriever.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/IntegratedCompressBlockDataRetriever.h"
#include "indexlib/file_system/file/NoCompressBlockDataRetriever.h"
#include "indexlib/file_system/file/NormalCompressBlockDataRetriever.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/cache/Block.h"
#include "indexlib/util/cache/BlockCache.h"

namespace autil { namespace mem_pool {
class Pool;
}} // namespace autil::mem_pool

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BlockByteSliceList);

BlockByteSliceList::BlockByteSliceList(const ReadOption& option, BlockFileAccessor* accessor) noexcept
    : _dataRetriever(nullptr)
    , _pool(nullptr)
{
    IE_SUB_CLASS_TYPE_SETUP(ByteSliceList, BlockByteSliceList);
    _dataRetriever = IE_POOL_COMPATIBLE_NEW_CLASS(_pool, NoCompressBlockDataRetriever, option, accessor);
}

BlockByteSliceList::BlockByteSliceList(const ReadOption& option, const std::shared_ptr<CompressFileInfo>& compressInfo,
                                       CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader,
                                       autil::mem_pool::Pool* pool) noexcept
    : _dataRetriever(nullptr)
    , _pool(pool)
{
    IE_SUB_CLASS_TYPE_SETUP(ByteSliceList, BlockByteSliceList);
    _dataRetriever = CreateCompressBlockDataRetriever(option, compressInfo, compressAddrMapper, dataFileReader, _pool);
}

BlockByteSliceList::~BlockByteSliceList() noexcept
{
    Clear(NULL);
    IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _dataRetriever);
}

void BlockByteSliceList::AddBlock(size_t fileOffset, size_t dataSize) noexcept
{
    ByteSlice* byteSlice = ByteSlice::CreateObject(0);
    byteSlice->next = NULL;
    byteSlice->data = NULL;
    byteSlice->size = dataSize;
    byteSlice->dataSize = 0;
    byteSlice->offset = fileOffset;
    Add(byteSlice);
}

void BlockByteSliceList::Clear(autil::mem_pool::Pool* pool) noexcept
{
    _dataRetriever->Reset();
    ByteSlice* slice = GetHead();
    ByteSlice* next = NULL;
    while (slice) {
        // set size = 0 to make the slice
        //             be destroyed via base class
        slice->size = 0;
        slice->data = NULL;
        slice->dataSize = 0;
        slice->offset = 0;
        next = slice->next;

        ByteSlice::DestroyObject(slice, pool);
        slice = next;
    }

    _head = _tail = NULL;
    _totalSize = 0;
}

FSResult<ByteSlice*> BlockByteSliceList::GetNextSlice(util::ByteSlice* slice) noexcept
{
    if (!slice) {
        return {FSEC_OK, nullptr};
    }
    if (slice->dataSize == slice->size) {
        return {FSEC_OK, (ByteSlice*)slice->next};
    }
    return GetSlice(slice->offset + slice->dataSize, slice);
}

FSResult<ByteSlice*> BlockByteSliceList::GetSlice(size_t fileOffset, ByteSlice* slice) noexcept
{
    if (!slice) {
        return {FSEC_OK, nullptr};
    }
    assert(fileOffset >= slice->offset && fileOffset < slice->offset + slice->size);
    assert(slice->dataSize <= slice->size);
    if (fileOffset >= slice->offset + slice->dataSize) {
        assert(_dataRetriever);
        size_t blockOffset;
        size_t blockDataSize;
        FSResult<uint8_t*> blockDataRet = _dataRetriever->RetrieveBlockData(fileOffset, blockOffset, blockDataSize);
        RETURN2_IF_FS_ERROR(blockDataRet.Code(), nullptr, "retrieve block failed");
        uint8_t* blockData = blockDataRet.Value();
        assert(blockData);
        ByteSlice* retSlice = nullptr;
        if (blockOffset <= slice->offset) {
            auto dataOffset = slice->offset - blockOffset;
            slice->data = blockData + dataOffset;
            slice->dataSize = blockDataSize - dataOffset;
            retSlice = slice;
        } else {
            auto newSlice = ByteSlice::CreateObject(0);
            newSlice->offset = blockOffset;
            newSlice->size = slice->offset + slice->size - newSlice->offset;
            newSlice->data = blockData;
            newSlice->dataSize = blockDataSize;
            newSlice->next = slice->next;
            slice->size = slice->size - newSlice->size;
            slice->next = newSlice;
            if (_tail == slice) {
                _tail = newSlice;
            }
            retSlice = newSlice;
        }
        retSlice->dataSize = std::min(static_cast<size_t>(retSlice->dataSize), static_cast<size_t>(retSlice->size));
        return {FSEC_OK, retSlice};
    }
    return {FSEC_OK, slice};
}

BlockDataRetriever* BlockByteSliceList::CreateCompressBlockDataRetriever(
    const ReadOption& option, const std::shared_ptr<CompressFileInfo>& compressInfo,
    CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader, autil::mem_pool::Pool* pool)
{
    auto fileNode = dataFileReader->GetFileNode();
    auto fileNodeType = fileNode->GetType();
    switch (fileNodeType) {
    case FSFT_MEM:
    case FSFT_MMAP:
    case FSFT_MMAP_LOCK: {
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IntegratedCompressBlockDataRetriever, option, compressInfo,
                                            compressAddrMapper, dataFileReader, pool);
    }
    case FSFT_BLOCK: {
        BlockFileNode* blockFileNode = (BlockFileNode*)(fileNode.get());
        if (blockFileNode->EnableCacheDecompressFile() &&
            (blockFileNode->GetBlockCache()->GetBlockSize() % compressInfo->blockSize) == 0) {
            return IE_POOL_COMPATIBLE_NEW_CLASS(pool, DecompressCachedBlockDataRetriever, option, compressInfo,
                                                compressAddrMapper, dataFileReader, pool);
        }
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, NormalCompressBlockDataRetriever, option, compressInfo,
                                            compressAddrMapper, dataFileReader, pool);
    }
    default:
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, NormalCompressBlockDataRetriever, option, compressInfo,
                                            compressAddrMapper, dataFileReader, pool);
    }
    assert(false);
    return nullptr;
}

future_lite::coro::Lazy<bool> BlockByteSliceList::Prefetch(size_t length) noexcept
{
    if (!_head) {
        co_return false;
    }
    auto result = co_await _dataRetriever->Prefetch(_head->offset, length);
    co_return result == ErrorCode::FSEC_OK;
}
}} // namespace indexlib::file_system
