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
#include "indexlib/file_system/file/NormalFileReader.h"

#include <assert.h>
#include <cstddef>
#include <stdint.h>

#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/util/cache/BlockAccessCounter.h"

using namespace std;
using namespace indexlib::util;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, NormalFileReader);

NormalFileReader::NormalFileReader(const std::shared_ptr<FileNode>& fileNode) noexcept : _fileNode(fileNode)
{
    assert(fileNode);
}

NormalFileReader::~NormalFileReader() noexcept
{
    [[maybe_unused]] auto ret = Close();
    assert(ret.OK());
}

FSResult<void> NormalFileReader::Open() noexcept
{
    RETURN_IF_FS_ERROR(_fileNode->Populate(), "populate fileNode[%s] failed", _fileNode->DebugString().c_str());
    return FSEC_OK;
}

FSResult<size_t> NormalFileReader::Read(void* buffer, size_t length, size_t offset, ReadOption option) noexcept
{
    auto [ec, readSize] = _fileNode->Read(buffer, length, offset, option);
    _offset = (offset + readSize);
    return {ec, readSize};
}

future_lite::Future<FSResult<size_t>> NormalFileReader::ReadAsync(void* buffer, size_t length, size_t offset,
                                                                  ReadOption option) noexcept
{
    return _fileNode->ReadAsync(buffer, length, offset, option);
}

FSResult<size_t> NormalFileReader::Read(void* buffer, size_t length, ReadOption option) noexcept
{
    auto [ec, readSize] = _fileNode->Read(buffer, length, (size_t)_offset, option);
    _offset += (int64_t)readSize;
    return {ec, readSize};
}

ByteSliceList* NormalFileReader::ReadToByteSliceList(size_t length, size_t offset, ReadOption option) noexcept
{
    return _fileNode->ReadToByteSliceList(length, offset, option);
}

void* NormalFileReader::GetBaseAddress() const noexcept { return _fileNode->GetBaseAddress(); }

size_t NormalFileReader::GetLength() const noexcept { return _fileNode->GetLength(); }

const string& NormalFileReader::GetLogicalPath() const noexcept { return _fileNode->GetLogicalPath(); }

const std::string& NormalFileReader::GetPhysicalPath() const noexcept { return _fileNode->GetPhysicalPath(); }

FSOpenType NormalFileReader::GetOpenType() const noexcept { return _fileNode->GetOpenType(); }

FSFileType NormalFileReader::GetType() const noexcept { return _fileNode->GetType(); };

FSResult<void> NormalFileReader::Close() noexcept { return FSEC_OK; }

FSResult<size_t> NormalFileReader::Prefetch(size_t length, size_t offset, ReadOption option) noexcept
{
    return _fileNode->Prefetch(length, offset, option);
}

future_lite::Future<FSResult<size_t>> NormalFileReader::PrefetchAsync(size_t length, size_t offset,
                                                                      ReadOption option) noexcept
{
    return _fileNode->PrefetchAsync(length, offset, option);
}

future_lite::Future<FSResult<uint32_t>> NormalFileReader::ReadVUInt32Async(size_t offset, ReadOption option) noexcept
{
    return _fileNode->ReadVUInt32Async(offset, option);
}
}} // namespace indexlib::file_system
