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
#include <vector>

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/file/BlockDataRetriever.h"
#include "indexlib/file_system/file/BlockPrefetcher.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace autil::mem_pool {
class Pool;
} // namespace autil::mem_pool

namespace indexlib::file_system {
class BlockByteSliceList;
class FileReader;
class CompressFileInfo;
class CompressFileAddressMapper;
class BlockFileAccessor;
} // namespace indexlib::file_system

namespace indexlib::util {
class BlockCache;
class BufferCompressor;
IE_SUB_CLASS_TYPE_DECLARE_WITH_NS(ByteSliceList, BlockByteSliceList, file_system);
} // namespace indexlib::util

namespace indexlib::file_system {
class BlockByteSliceList final : public util::ByteSliceList
{
public:
    BlockByteSliceList(const ReadOption& option, BlockFileAccessor* accessor) noexcept;

    BlockByteSliceList(const ReadOption& option, const std::shared_ptr<CompressFileInfo>& compressInfo,
                       CompressFileAddressMapper* compressAddrMapper, FileReader* dataFileReader,
                       autil::mem_pool::Pool* pool) noexcept;

    ~BlockByteSliceList() noexcept;

public:
    void InitMetricsReporter(const DecompressMetricsReporter& reporter)
    {
        assert(_dataRetriever);
        _dataRetriever->SetMetricsReporter(reporter);
    }

    void AddBlock(size_t fileOffset, size_t dataSize) noexcept;
    void Clear(autil::mem_pool::Pool* pool) noexcept override;
    future_lite::coro::Lazy<bool> Prefetch(size_t length) noexcept override;
    util::ByteSlice* GetNextSlice(util::ByteSlice* slice);
    util::ByteSlice* GetSlice(size_t offset, util::ByteSlice* slice);

private:
    BlockDataRetriever* CreateCompressBlockDataRetriever(const ReadOption& option,
                                                         const std::shared_ptr<CompressFileInfo>& compressInfo,
                                                         CompressFileAddressMapper* compressAddrMapper,
                                                         FileReader* dataFileReader, autil::mem_pool::Pool* pool);

private:
    BlockDataRetriever* _dataRetriever;
    autil::mem_pool::Pool* _pool;

private:
    friend class BlockByteSliceListTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BlockByteSliceList> BlockByteSliceListPtr;
} // namespace indexlib::file_system
