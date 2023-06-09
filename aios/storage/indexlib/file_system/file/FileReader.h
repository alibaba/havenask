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

#include <assert.h>
#include <cstdint>
#include <stddef.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "future_lite/Helper.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/CoroutineConfig.h"

namespace indexlib::util {
class ByteSliceList;
}

namespace indexlib { namespace file_system {
class FileNode;
class FileSystemMetricsReporter;

class FileReader
{
public:
    FileReader() noexcept;
    virtual ~FileReader() noexcept;

public:
    virtual FSResult<void> Open() noexcept = 0;
    virtual FSResult<void> Close() noexcept = 0;
    virtual FSResult<size_t> Read(void* buffer, size_t length, size_t offset,
                                  ReadOption option = ReadOption()) noexcept = 0;
    virtual FSResult<size_t> Read(void* buffer, size_t length, ReadOption option = ReadOption()) noexcept = 0;
    virtual util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept = 0;
    virtual void* GetBaseAddress() const noexcept = 0;
    virtual size_t GetLength() const noexcept = 0;

    // logic length match with offset, which is equal to uncompress file length for compress file
    virtual const std::string& GetLogicalPath() const noexcept = 0;
    virtual const std::string& GetPhysicalPath() const noexcept = 0;
    virtual FSOpenType GetOpenType() const noexcept = 0;
    virtual FSFileType GetType() const noexcept = 0;
    virtual std::shared_ptr<FileNode> GetFileNode() const noexcept = 0;

    virtual FSResult<size_t> Prefetch(size_t length, size_t offset, ReadOption option) noexcept;
    virtual size_t GetLogicLength() const noexcept { return GetLength(); }

    // future_lite
    virtual future_lite::Future<size_t> ReadAsync(void* buffer, size_t length, size_t offset,
                                                  ReadOption option) noexcept(false);
    virtual future_lite::Future<uint32_t> ReadUInt32Async(size_t offset, ReadOption option) noexcept(false);
    virtual future_lite::Future<uint32_t> ReadVUInt32Async(ReadOption option) noexcept(false);
    virtual future_lite::Future<uint32_t> ReadVUInt32Async(size_t offset, ReadOption option) noexcept(false);
    virtual future_lite::Future<size_t> PrefetchAsync(size_t length, size_t offset, ReadOption option) noexcept(false);

    // FL_LAZY
    virtual FL_LAZY(FSResult<size_t>)
        ReadAsyncCoro(void* buffer, size_t length, size_t offset, ReadOption option) noexcept;
    virtual FL_LAZY(FSResult<size_t>) PrefetchAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept;

    future_lite::coro::Lazy<std::vector<FSResult<size_t>>> BatchRead(const BatchIO& batchIO,
                                                                     ReadOption option) noexcept;
    FSResult<void> Seek(int64_t offset) noexcept;
    int64_t Tell() const noexcept { return _offset; }

public:
    FSResult<uint32_t> ReadVUInt32(ReadOption option = ReadOption()) noexcept;
    FSResult<uint32_t> ReadVUInt32(size_t offset, ReadOption option = ReadOption()) noexcept;
    bool IsMemLock() const noexcept;

public:
    std::string TEST_Load() noexcept(false);
    std::string DebugString() const noexcept;
    void InitMetricReporter(FileSystemMetricsReporter* reporter) noexcept;

protected:
    /*offset of batchIO is non-decreasing*/
    virtual future_lite::coro::Lazy<std::vector<FSResult<size_t>>> BatchReadOrdered(const BatchIO& batchIO,
                                                                                    ReadOption option) noexcept;

protected:
    int64_t _offset = 0;

private:
    friend class LocalStorageTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileReader> FileReaderPtr;

//////////////////////////////////////////////////////////////////////

inline future_lite::coro::Lazy<std::vector<FSResult<size_t>>> FileReader::BatchRead(const BatchIO& batchIO,
                                                                                    ReadOption option) noexcept
{
    size_t fileLength = GetLogicLength();
    if (std::any_of(batchIO.begin(), batchIO.end(), [&fileLength, this](const SingleIO& io) {
            if (io.offset + io.len > fileLength) {
                AUTIL_LOG(ERROR, "read file [%s] out of range, offset [%lu], length [%lu], fileLength [%lu]",
                          DebugString().c_str(), io.offset, io.len, fileLength);
                return true;
            }
            return false;
        })) {
        co_return std::vector<FSResult<size_t>>(batchIO.size(), {ErrorCode::FSEC_ERROR, 0});
    }
    if (std::is_sorted(batchIO.begin(), batchIO.end())) {
        co_return co_await BatchReadOrdered(batchIO, option);
    }
    std::vector<size_t> sortIndex(batchIO.size());
    for (size_t i = 0; i < batchIO.size(); ++i) {
        sortIndex[i] = i;
    }
    std::sort(sortIndex.begin(), sortIndex.end(),
              [&batchIO](size_t lhs, size_t rhs) { return batchIO[lhs] < batchIO[rhs]; });
    BatchIO sortedBatchIO(batchIO.size());
    for (size_t i = 0; i < batchIO.size(); ++i) {
        sortedBatchIO[i] = batchIO[sortIndex[i]];
    }

    auto sortedResult = co_await BatchReadOrdered(sortedBatchIO, option);
    std::vector<FSResult<size_t>> realResult(batchIO.size());
    for (size_t i = 0; i < batchIO.size(); ++i) {
        realResult[sortIndex[i]] = sortedResult[i];
    }
    co_return realResult;
}

inline future_lite::coro::Lazy<std::vector<FSResult<size_t>>> FileReader::BatchReadOrdered(const BatchIO& batchIO,
                                                                                           ReadOption option) noexcept
{
    std::vector<FSResult<size_t>> result(batchIO.size());
    for (size_t i = 0; i < batchIO.size(); ++i) {
        auto& singleIO = batchIO[i];
        result[i] = Read(singleIO.buffer, singleIO.len, singleIO.offset, option);
    }
    co_return result;
}

inline future_lite::Future<size_t> FileReader::ReadAsync(void* buffer, size_t length, size_t offset,
                                                         ReadOption option) noexcept(false)
{
    return future_lite::makeReadyFuture(Read(buffer, length, offset, option).GetOrThrow());
}

inline FL_LAZY(FSResult<size_t>) FileReader::ReadAsyncCoro(void* buffer, size_t length, size_t offset,
                                                           ReadOption option) noexcept
{
    FL_CORETURN Read(buffer, length, offset, option);
}

inline future_lite::Future<uint32_t> FileReader::ReadUInt32Async(size_t offset, ReadOption option) noexcept(false)
{
    uint32_t buffer;
    auto readSize = Read(static_cast<void*>(&buffer), sizeof(buffer), offset, option).GetOrThrow();
    assert(readSize == sizeof(buffer));
    (void)readSize;
    return future_lite::makeReadyFuture<uint32_t>(buffer);
}

inline FSResult<uint32_t> FileReader::ReadVUInt32(ReadOption option) noexcept
{
    uint8_t byte;
    RETURN2_IF_FS_ERROR(Read((void*)&byte, sizeof(byte), option).Code(), 0u, "Read failed");
    uint32_t value = byte & 0x7F;
    int shift = 7;

    while (byte & 0x80) {
        RETURN2_IF_FS_ERROR(Read((void*)&byte, sizeof(byte), option).Code(), 0u, "Read failed");
        value |= ((byte & 0x7F) << shift);
        shift += 7;
    }
    return {FSEC_OK, value};
}

inline FSResult<uint32_t> FileReader::ReadVUInt32(size_t offset, ReadOption option) noexcept
{
    uint8_t byte;
    RETURN2_IF_FS_ERROR(Read((void*)&byte, sizeof(byte), offset++, option).Code(), 0u, "Read failed");
    uint32_t value = byte & 0x7F;
    int shift = 7;

    while (byte & 0x80) {
        RETURN2_IF_FS_ERROR(Read((void*)&byte, sizeof(byte), offset++, option).Code(), 0u, "Read failed");
        value |= ((byte & 0x7F) << shift);
        shift += 7;
    }
    return {FSEC_OK, value};
}

inline future_lite::Future<uint32_t> FileReader::ReadVUInt32Async(ReadOption option) noexcept(false)
{
    return future_lite::makeReadyFuture(ReadVUInt32(option).GetOrThrow());
}

inline future_lite::Future<uint32_t> FileReader::ReadVUInt32Async(size_t offset, ReadOption option) noexcept(false)
{
    return future_lite::makeReadyFuture(ReadVUInt32(offset, option).GetOrThrow());
}

inline future_lite::Future<size_t> FileReader::PrefetchAsync(size_t length, size_t offset,
                                                             ReadOption option) noexcept(false)
{
    return future_lite::makeReadyFuture<size_t>(0);
}

inline FL_LAZY(FSResult<size_t>) FileReader::PrefetchAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept
{
    FL_CORETURN FSResult<size_t> {FSEC_OK, 0ul};
}
}} // namespace indexlib::file_system
