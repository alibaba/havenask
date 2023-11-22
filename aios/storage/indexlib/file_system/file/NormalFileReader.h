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

#include <cstdint>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/Future.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace file_system {

class NormalFileReader : public FileReader
{
public:
    NormalFileReader(const std::shared_ptr<FileNode>& fileNode) noexcept;
    ~NormalFileReader() noexcept;

public:
    FSResult<void> Open() noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    FSResult<size_t> Read(void* buffer, size_t length, ReadOption option) noexcept override;
    util::ByteSliceList* ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept override;
    void* GetBaseAddress() const noexcept override;
    size_t GetLength() const noexcept override;
    const std::string& GetLogicalPath() const noexcept override;
    const std::string& GetPhysicalPath() const noexcept override;
    FSOpenType GetOpenType() const noexcept override;
    FSFileType GetType() const noexcept override;
    FSResult<void> Close() noexcept override;
    FSResult<size_t> Prefetch(size_t length, size_t offset, ReadOption option) noexcept override;
    std::shared_ptr<FileNode> GetFileNode() const noexcept override { return _fileNode; }

    // future_lite
    future_lite::Future<FSResult<size_t>> ReadAsync(void* buffer, size_t length, size_t offset,
                                                    ReadOption option) noexcept override;
    future_lite::Future<FSResult<uint32_t>> ReadVUInt32Async(size_t offset, ReadOption option) noexcept override;
    future_lite::Future<FSResult<uint32_t>> ReadUInt32Async(size_t offset, ReadOption option) noexcept override;
    future_lite::Future<FSResult<size_t>> PrefetchAsync(size_t length, size_t offset,
                                                        ReadOption option) noexcept override;

    // FL_LAZY
    FL_LAZY(FSResult<size_t>)
    ReadAsyncCoro(void* buffer, size_t length, size_t offset, ReadOption option) noexcept override;
    FL_LAZY(FSResult<size_t>) PrefetchAsyncCoro(size_t length, size_t offset, ReadOption option) noexcept override;

private:
    future_lite::coro::Lazy<std::vector<FSResult<size_t>>> BatchReadOrdered(const BatchIO& batchIO,
                                                                            ReadOption option) noexcept override;

private:
    std::shared_ptr<FileNode> _fileNode;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<NormalFileReader> NormalFileReaderPtr;

//////////////////////////////////////////////////////////////////////
inline future_lite::coro::Lazy<std::vector<FSResult<size_t>>>
NormalFileReader::BatchReadOrdered(const BatchIO& batchIO, ReadOption option) noexcept
{
    co_return co_await _fileNode->BatchReadOrdered(batchIO, option);
}

inline FL_LAZY(FSResult<size_t>) NormalFileReader::ReadAsyncCoro(void* buffer, size_t length, size_t offset,
                                                                 ReadOption option) noexcept
{
    FL_CORETURN FL_COAWAIT _fileNode->ReadAsyncCoro(buffer, length, offset, option);
}

inline FL_LAZY(FSResult<size_t>) NormalFileReader::PrefetchAsyncCoro(size_t length, size_t offset,
                                                                     ReadOption option) noexcept
{
    FL_CORETURN FL_COAWAIT _fileNode->PrefetchAsyncCoro(length, offset, option);
}

inline future_lite::Future<FSResult<uint32_t>> NormalFileReader::ReadUInt32Async(size_t offset,
                                                                                 ReadOption option) noexcept
{
    return _fileNode->ReadUInt32Async(offset, option);
}

}} // namespace indexlib::file_system
