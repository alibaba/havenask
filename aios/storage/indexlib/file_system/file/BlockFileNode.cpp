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
#include "indexlib/file_system/file/BlockFileNode.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <exception>
#include <iosfwd>
#include <string.h>
#include <string>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "fslib/common/common_type.h"
#include "future_lite/Common.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "future_lite/Helper.h"
#include "indexlib/file_system/FileBlockCache.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/BlockByteSliceList.h"
#include "indexlib/file_system/file/BlockFileAccessor.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/fslib/FslibOption.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/cache/BlockHandle.h"

namespace indexlib {
namespace file_system {
class PackageOpenMeta;
} // namespace file_system
namespace util {
class BlockCache;
} // namespace util
} // namespace indexlib

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, BlockFileNode);

BlockFileNode::BlockFileNode(util::BlockCache* blockCache, bool useDirectIO, bool cacheDecompressFile,
                             bool useHighPriority, const std::string& linkRoot) noexcept
    : _accessor(blockCache, useDirectIO, useHighPriority, linkRoot)
    , _cacheDecompressFile(cacheDecompressFile)
    , _useHighPriority(useHighPriority)
    , _cacheDecompressFileId(0)
{
}

BlockFileNode::~BlockFileNode() noexcept {}

ErrorCode BlockFileNode::DoOpen(const PackageOpenMeta& packageOpenMeta, FSOpenType openType) noexcept
{
    RETURN_IF_FS_ERROR(_accessor.Open(GetLogicalPath(), packageOpenMeta), "DoOpen failed");
    if (_cacheDecompressFile) {
        string pathForCache = GetPhysicalPath() + "#" + GetLogicalPath() + "@decompress_in_cache";
        _cacheDecompressFileId = FileBlockCache::GetFileId(pathForCache);
    }
    return FSEC_OK;
}

ErrorCode BlockFileNode::DoOpen(const string& path, FSOpenType openType, int64_t fileLength) noexcept
{
    RETURN_IF_FS_ERROR(_accessor.Open(path, fileLength), "DoOpen failed");
    if (_cacheDecompressFile) {
        string pathForCache = GetPhysicalPath() + "#" + GetLogicalPath() + "@decompress_in_cache";
        _cacheDecompressFileId = FileBlockCache::GetFileId(pathForCache);
    }
    return FSEC_OK;
}

FSFileType BlockFileNode::GetType() const noexcept { return FSFT_BLOCK; }

size_t BlockFileNode::GetLength() const noexcept { return _accessor.GetFileLength(); }

void* BlockFileNode::GetBaseAddress() const noexcept
{
    AUTIL_LOG(DEBUG, "not supported GetBaseAddress");
    return NULL;
}

FSResult<size_t> BlockFileNode::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    if ((offset + length) > _accessor.GetFileLength()) {
        AUTIL_LOG(ERROR, "read file out of range, offset: [%lu], read length: [%lu], file length: [%lu]", offset,
                  length, _accessor.GetFileLength());
        return {FSEC_BADARGS, 0};
    }
    option.advice = IO_ADVICE_LOW_LATENCY;
    return _accessor.Read(buffer, length, offset, option);
}

future_lite::Future<FSResult<size_t>> BlockFileNode::ReadAsync(void* buffer, size_t length, size_t offset,
                                                               ReadOption option) noexcept
{
    if ((offset + length) > _accessor.GetFileLength()) {
        AUTIL_LOG(ERROR,
                  "read file [%s] out of range, offset: [%lu], "
                  "read length: [%lu], file length: [%lu]",
                  DebugString().c_str(), offset, length, _accessor.GetFileLength());
        return future_lite::makeReadyFuture<FSResult<size_t>>({FSEC_ERROR, 0});
    }

    return _accessor.ReadAsync(buffer, length, offset, option);
}

FL_LAZY(FSResult<size_t>)
BlockFileNode::ReadAsyncCoro(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    // TODO(xinfei.sxf) add exception
    if ((offset + length) > _accessor.GetFileLength()) {
        AUTIL_LOG(ERROR, "read file out of range, offset: [%lu], read length: [%lu], file length: [%lu]", offset,
                  length, _accessor.GetFileLength());
        FL_CORETURN FSResult<size_t> {FSEC_BADARGS, 0ul};
    }

    FL_CORETURN FL_COAWAIT _accessor.ReadAsyncCoro(buffer, length, offset, option);
}

ByteSliceList* BlockFileNode::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    if (offset + length > _accessor.GetFileLength()) {
        AUTIL_LOG(ERROR,
                  "read file [%s] out of range, offset: [%lu], "
                  "read length: [%lu], file length: [%lu]",
                  DebugString().c_str(), offset, length, _accessor.GetFileLength());
        return nullptr;
    }

    BlockByteSliceList* sliceList = new BlockByteSliceList(option, &_accessor);
    auto sliceLen = std::min(length, _accessor.GetFileLength() - offset);
    sliceList->AddBlock(offset, sliceLen);
    return sliceList;
}

FSResult<size_t> BlockFileNode::Write(const void* buffer, size_t length) noexcept
{
    AUTIL_LOG(ERROR, "not supported Write");
    return {FSEC_NOTSUP, 0};
}

FSResult<void> BlockFileNode::Close() noexcept { return _accessor.Close(); }

FSResult<size_t> BlockFileNode::Prefetch(size_t length, size_t offset, ReadOption option) noexcept
{
    size_t fileLength = _accessor.GetFileLength();
    if (offset + length > fileLength) {
        if (unlikely(offset >= fileLength)) {
            return {FSEC_OK, 0};
        }
        length = fileLength - offset;
    }
    return _accessor.Prefetch(length, offset, option);
}

future_lite::Future<FSResult<uint32_t>>
BlockFileNode::DoReadUInt32Async(size_t offset, size_t leftBytes, uint32_t currentValue, ReadOption option) noexcept
{
    assert(leftBytes <= sizeof(uint32_t));
    return _accessor.GetBlockAsync(offset, option)
        .thenValue([this, offset, leftBytes, currentValue,
                    option](FSResult<BlockHandle>&& ret) mutable -> future_lite::Future<FSResult<uint32_t>> {
            RETURN_RESULT_IF_FS_ERROR(ret.Code(), future_lite::makeReadyFuture<FSResult<uint32_t>>({ret.Code(), 0u}),
                                      "GetBlockAsync failed");
            const auto& handle = ret.Value();
            auto data = handle.GetData();
            size_t blockSize = handle.GetDataSize();
            size_t inBlockOffset = offset - handle.GetOffset();
            assert(inBlockOffset < blockSize);
            size_t copySize = std::min(blockSize - inBlockOffset, leftBytes);
            memcpy(reinterpret_cast<char*>(&currentValue) + sizeof(uint32_t) - leftBytes, data + inBlockOffset,
                   copySize);
            if (copySize != leftBytes) {
                return DoReadUInt32Async(offset + copySize, leftBytes - copySize, currentValue, option);
            }
            return future_lite::makeReadyFuture<FSResult<uint32_t>>({FSEC_OK, currentValue});
        });
}

FL_LAZY(FSResult<uint32_t>)
BlockFileNode::DoReadUInt32AsyncCoro(size_t offset, size_t leftBytes, uint32_t currentValue, ReadOption option) noexcept
{
    assert(leftBytes <= sizeof(uint32_t));
    auto [ec, handle] = FL_COAWAIT _accessor.GetBlockAsyncCoro(offset, option);
    FL_CORETURN2_IF_FS_ERROR(ec, 0u, "GetBlockAsyncCoro failed, offset[%lu]", offset);
    auto data = handle.GetData();
    size_t blockSize = handle.GetDataSize();
    size_t inBlockOffset = offset - handle.GetOffset();
    assert(inBlockOffset < blockSize);
    size_t copySize = std::min(blockSize - inBlockOffset, leftBytes);
    memcpy(reinterpret_cast<char*>(&currentValue) + sizeof(uint32_t) - leftBytes, data + inBlockOffset, copySize);
    if (copySize != leftBytes) {
        FL_CORETURN FL_COAWAIT DoReadUInt32AsyncCoro(offset + copySize, leftBytes - copySize, currentValue, option);
    }
    FL_CORETURN FSResult<uint32_t> {FSEC_OK, currentValue};
}

future_lite::Future<FSResult<size_t>> BlockFileNode::PrefetchAsync(size_t length, size_t offset,
                                                                   ReadOption option) noexcept
{
    size_t fileLength = _accessor.GetFileLength();
    if (offset + length > fileLength) {
        if (unlikely(offset >= fileLength)) {
            return future_lite::makeReadyFuture<FSResult<size_t>>({FSEC_OK, 0ul});
        }
        length = fileLength - offset;
    }
    return _accessor.PrefetchAsync(length, offset, option);
}

FL_LAZY(FSResult<size_t>) BlockFileNode::PrefetchAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept
{
    size_t fileLength = _accessor.GetFileLength();
    if (offset + length > fileLength) {
        if (unlikely(offset >= fileLength)) {
            FL_CORETURN FSResult<size_t> {FSEC_BADARGS, 0ul};
        }
        length = fileLength - offset;
    }
    FL_CORETURN FL_COAWAIT _accessor.PrefetchAsyncCoro(length, offset, option);
}

future_lite::Future<FSResult<uint32_t>> BlockFileNode::ReadUInt32Async(size_t offset, ReadOption option) noexcept
{
    if (unlikely((offset + sizeof(uint32_t)) > _accessor.GetFileLength())) {
        AUTIL_LOG(ERROR,
                  "read file [%s] out of range, offset: [%lu], "
                  "read length: 4, file length: [%lu]",
                  DebugString().c_str(), offset, _accessor.GetFileLength());
        return future_lite::makeReadyFuture<FSResult<uint32_t>>({FSEC_ERROR, 0});
    }
    return DoReadUInt32Async(offset, sizeof(uint32_t), 0, option);
}

FL_LAZY(FSResult<uint32_t>) BlockFileNode::ReadUInt32AsyncCoro(size_t offset, ReadOption option) noexcept
{
    if (unlikely((offset + sizeof(uint32_t)) > _accessor.GetFileLength())) {
        AUTIL_LOG(ERROR, "read file out of range, offset: [%lu], read length: 4, file length: [%lu]", offset,
                  _accessor.GetFileLength());
        FL_CORETURN FSResult<uint32_t> {FSEC_BADARGS, 0u};
    }
    FL_CORETURN FL_COAWAIT DoReadUInt32AsyncCoro(offset, sizeof(uint32_t), 0, option);
}

future_lite::coro::Lazy<vector<FSResult<size_t>>> BlockFileNode::BatchReadOrdered(const BatchIO& batchIO,
                                                                                  ReadOption option) noexcept
{
    assert(is_sorted(batchIO.begin(), batchIO.end()));
    assert(batchIO.empty() || (batchIO.rbegin()->len + batchIO.rbegin()->offset <= GetLength()));
    co_return co_await _accessor.BatchReadOrdered(batchIO, option);
}

void BlockFileNode::InitMetricReporter(FileSystemMetricsReporter* reporter) noexcept
{
    FileNode::UpdateFileLengthMetric(reporter);
    _accessor.InitTagMetricReporter(reporter, GetLogicalPath());
}

}} // namespace indexlib::file_system
