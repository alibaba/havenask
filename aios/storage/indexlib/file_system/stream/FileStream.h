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
#include "autil/mem_pool/Pool.h"
#include "future_lite/Future.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/util/CoroutineConfig.h"

namespace indexlib::file_system {

class FileStream
{
public:
    FileStream(size_t streamLength, bool supportConcurrency)
        : _streamLength(streamLength)
        , _supportConcurrency(supportConcurrency)
    {
    }
    virtual ~FileStream() {}

    FileStream(const FileStream&) = delete;
    FileStream& operator=(const FileStream&) = delete;
    FileStream(FileStream&&) = delete;
    FileStream& operator=(FileStream&&) = delete;

    // general interface
public:
    virtual FSResult<size_t> Read(void* buffer, size_t length, size_t offset, file_system::ReadOption option) = 0;
    virtual future_lite::coro::Lazy<std::vector<file_system::FSResult<size_t>>>
    BatchRead(file_system::BatchIO& batchIO, file_system::ReadOption option) noexcept = 0;

    virtual future_lite::Future<size_t> ReadAsync(void* buffer, size_t length, size_t offset,
                                                  file_system::ReadOption option) = 0;

    // create a file stream from this, which maybe not support concurrency read
    // the result from read is same with this
    virtual std::shared_ptr<FileStream> CreateSessionStream(autil::mem_pool::Pool* pool) const = 0;

    // Get the stream (data) length, instead of file length
    size_t GetStreamLength() const { return _streamLength; }

    // if true, support concurrency but maybe more performance overhead
    bool IsSupportConcurrency() const { return _supportConcurrency; }

    virtual std::string DebugString() const = 0;

    virtual size_t GetLockedMemoryUse() const = 0;
    // no-concurrency (thread-unsafe) interface

protected:
    size_t _streamLength;
    bool _supportConcurrency;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::file_system
