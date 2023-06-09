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
#include "indexlib/file_system/stream/CompressFileStream.h"

#include "autil/memory.h"
#include "indexlib/file_system/stream/FileStreamCreator.h"

namespace indexlib::file_system {
AUTIL_LOG_SETUP(indexlib.file_system, CompressFileStream);

CompressFileStream::CompressFileStream(const std::shared_ptr<file_system::CompressFileReader>& fileReader,
                                       bool supportConcurrency, autil::mem_pool::Pool* pool)
    : FileStream(fileReader->GetUncompressedFileLength(), supportConcurrency)
{
    _fileReader = autil::shared_ptr_pool(pool, fileReader->CreateSessionReader(pool));
}

CompressFileStream::~CompressFileStream() {}

FSResult<size_t> CompressFileStream::Read(void* buffer, size_t length, size_t offset,
                                          file_system::ReadOption option) noexcept
{
    std::shared_ptr<file_system::CompressFileReader> fileReader = _fileReader;
    if (IsSupportConcurrency()) {
        // TODO (yiping.typ) : maybe use pool is better
        fileReader.reset(_fileReader->CreateSessionReader(nullptr));
    }
    return fileReader->Read(buffer, length, offset, option);
}

future_lite::Future<size_t> CompressFileStream::ReadAsync(void* buffer, size_t length, size_t offset,
                                                          file_system::ReadOption option)
{
    std::shared_ptr<file_system::CompressFileReader> fileReader = _fileReader;
    if (IsSupportConcurrency()) {
        // TODO (yiping.typ) : maybe use pool is better
        fileReader.reset(_fileReader->CreateSessionReader(nullptr));
    }
    return fileReader->ReadAsync(buffer, length, offset, option);
}

future_lite::coro::Lazy<std::vector<file_system::FSResult<size_t>>>
CompressFileStream::BatchRead(file_system::BatchIO& batchIO, file_system::ReadOption option) noexcept
{
    std::shared_ptr<file_system::CompressFileReader> fileReader = _fileReader;
    if (IsSupportConcurrency()) {
        // TODO (yiping.typ) : maybe use pool is better
        fileReader.reset(_fileReader->CreateSessionReader(nullptr));
    }
    return fileReader->BatchRead(batchIO, option);
}

std::shared_ptr<FileStream> CompressFileStream::CreateSessionStream(autil::mem_pool::Pool* pool) const
{
    return FileStreamCreator::CreateFileStream(_fileReader, pool);
}

std::string CompressFileStream::DebugString() const { return _fileReader->DebugString(); }

size_t CompressFileStream::GetLockedMemoryUse() const
{
    return _fileReader->IsMemLock() ? _fileReader->GetLength() : 0;
}

} // namespace indexlib::file_system
