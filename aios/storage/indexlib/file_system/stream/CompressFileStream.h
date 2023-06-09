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

#include "autil/Log.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/file_system/stream/FileStream.h"

namespace indexlib::file_system {

class CompressFileStream : public FileStream
{
public:
    CompressFileStream(const std::shared_ptr<file_system::CompressFileReader>& fileReader, bool supportConcurrency,
                       autil::mem_pool::Pool* pool);
    ~CompressFileStream();

    CompressFileStream(const CompressFileStream&) = delete;
    CompressFileStream& operator=(const CompressFileStream&) = delete;
    CompressFileStream(CompressFileStream&&) = delete;
    CompressFileStream& operator=(CompressFileStream&&) = delete;

public:
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, file_system::ReadOption option) noexcept override;
    future_lite::Future<size_t> ReadAsync(void* buffer, size_t length, size_t offset,
                                          file_system::ReadOption option) override;
    future_lite::coro::Lazy<std::vector<file_system::FSResult<size_t>>>
    BatchRead(file_system::BatchIO& batchIO, file_system::ReadOption option) noexcept override;

    std::shared_ptr<FileStream> CreateSessionStream(autil::mem_pool::Pool* pool) const override;
    std::string DebugString() const override;
    size_t GetLockedMemoryUse() const override;

private:
    std::shared_ptr<file_system::CompressFileReader> _fileReader;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
