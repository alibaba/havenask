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
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/stream/FileStream.h"

namespace indexlib::file_system {

class NormalFileStream : public FileStream
{
public:
    NormalFileStream(const std::shared_ptr<FileReader>& fileReader, bool supportConcurrency);
    ~NormalFileStream();

    NormalFileStream(const NormalFileStream&) = delete;
    NormalFileStream& operator=(const NormalFileStream&) = delete;
    NormalFileStream(NormalFileStream&&) = delete;
    NormalFileStream& operator=(NormalFileStream&&) = delete;

public:
    FSResult<size_t> Read(void* buffer, size_t length, size_t offset, file_system::ReadOption option) noexcept override;
    future_lite::Future<size_t> ReadAsync(void* buffer, size_t length, size_t offset,
                                          file_system::ReadOption option) override;
    future_lite::coro::Lazy<std::vector<file_system::FSResult<size_t>>>
    BatchRead(file_system::BatchIO& batchIO, file_system::ReadOption option) noexcept override
    {
        co_return co_await _fileReader->BatchRead(batchIO, option);
    }
    std::shared_ptr<FileStream> CreateSessionStream(autil::mem_pool::Pool* pool) const override;
    std::string DebugString() const override;
    size_t GetLockedMemoryUse() const override;

private:
    std::shared_ptr<FileReader> _fileReader;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
