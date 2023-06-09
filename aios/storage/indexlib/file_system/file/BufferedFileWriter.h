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
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileWriterImpl.h"

namespace indexlib { namespace file_system {
class FileBuffer;
class FileNode;
class BufferedFileOutputStream;

class BufferedFileWriter : public FileWriterImpl
{
public:
    BufferedFileWriter(const WriterOption& writerOption = WriterOption::AtomicDump(),
                       UpdateFileSizeFunc&& updateFileSizeFunc = nullptr) noexcept;
    ~BufferedFileWriter() noexcept;

public:
    ErrorCode DoOpen() noexcept override;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    size_t GetLength() const noexcept override;
    ErrorCode DoClose() noexcept override;
    FSResult<void> ReserveFile(size_t reserveSize) noexcept override { return FSEC_OK; }
    FSResult<void> Truncate(size_t truncateSize) noexcept override
    {
        assert(false);
        return FSEC_OK;
    }
    void* GetBaseAddress() noexcept override
    {
        assert(false);
        return NULL;
    }

public:
    virtual FSResult<void> DumpBuffer() noexcept;
    FSResult<void> ResetBufferParam(size_t bufferSize, bool asyncWrite) noexcept;
    FSResult<bool> Flush() noexcept; // force flush

private:
    std::string GetDumpPath() const noexcept;

protected:
    std::shared_ptr<BufferedFileOutputStream> _stream;
    int64_t _length;
    bool _isClosed;
    std::unique_ptr<FileBuffer> _buffer;
    std::unique_ptr<FileBuffer> _switchBuffer;
    autil::ThreadPoolPtr _threadPool;

private:
    AUTIL_LOG_DECLARE();
    friend class BufferedFileWriterTest;
    friend class PartitionResourceProviderInteTest;
};

typedef std::shared_ptr<BufferedFileWriter> BufferedFileWriterPtr;
}} // namespace indexlib::file_system
