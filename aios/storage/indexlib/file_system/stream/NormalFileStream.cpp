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
#include "indexlib/file_system/stream/NormalFileStream.h"

#include "indexlib/file_system/stream/FileStreamCreator.h"

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, NormalFileStream);

NormalFileStream::NormalFileStream(const file_system::FileReaderPtr& fileReader, bool supportConcurrency)
    : FileStream(fileReader->GetLength(), supportConcurrency)
    , _fileReader(fileReader)
{
    assert(fileReader);
}

NormalFileStream::~NormalFileStream() {}

FSResult<size_t> NormalFileStream::Read(void* buffer, size_t length, size_t offset,
                                        file_system::ReadOption option) noexcept
{
    return _fileReader->Read(buffer, length, offset, option);
}

future_lite::Future<FSResult<size_t>> NormalFileStream::ReadAsync(void* buffer, size_t length, size_t offset,
                                                                  file_system::ReadOption option)
{
    return _fileReader->ReadAsync(buffer, length, offset, option);
}

std::shared_ptr<FileStream> NormalFileStream::CreateSessionStream(autil::mem_pool::Pool* pool) const
{
    return FileStreamCreator::CreateFileStream(_fileReader, pool);
}

std::string NormalFileStream::DebugString() const { return _fileReader->DebugString(); }

size_t NormalFileStream::GetLockedMemoryUse() const { return _fileReader->IsMemLock() ? _fileReader->GetLength() : 0; }

} // namespace indexlib::file_system
