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

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileReader.h"

namespace indexlib::util {
class ByteSliceList;
} // namespace indexlib::util

namespace indexlib { namespace file_system {
class FileBuffer;
class FileNode;
struct ReadOption;

class BufferedFileReader : public FileReader
{
public:
    BufferedFileReader(const std::shared_ptr<FileNode>& fileNode,
                       uint32_t bufferSize = ReaderOption::DEFAULT_BUFFER_SIZE) noexcept;
    ~BufferedFileReader() noexcept;

public:
    // Independent from the file_system
    BufferedFileReader(uint32_t bufferSize = ReaderOption::DEFAULT_BUFFER_SIZE, bool asyncRead = false) noexcept;
    FSResult<void> Open(const std::string& filePath) noexcept;

public:
    FSResult<void> Open() noexcept override { return FSEC_OK; }
    // The Read with offset should modify _offset
    // The result of Read without offset will be influenced
    // They can not be used interchangeably
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
    std::shared_ptr<FileNode> GetFileNode() const noexcept override { return _fileNode; }

public:
    void ResetBufferParam(size_t bufferSize, bool asyncRead) noexcept;

private:
    virtual FSResult<void> LoadBuffer(int64_t blockNum, ReadOption option) noexcept;

    FSResult<void> SyncLoadBuffer(int64_t blockNum, ReadOption option) noexcept;
    FSResult<void> AsyncLoadBuffer(int64_t blockNum, ReadOption option) noexcept;
    void PrefetchBlock(int64_t blockNum, ReadOption option) noexcept;

    virtual FSResult<size_t> DoRead(void* buffer, size_t length, size_t offset, ReadOption option) noexcept;

private:
    std::shared_ptr<FileNode> _fileNode;
    int64_t _fileLength;
    int64_t _curBlock;
    FileBuffer* _buffer;
    FileBuffer* _switchBuffer;
    autil::ThreadPoolPtr _threadPool;

private:
    AUTIL_LOG_DECLARE();
    friend class MockBufferedFileReader;
};

typedef std::shared_ptr<BufferedFileReader> BufferedFileReaderPtr;
}} // namespace indexlib::file_system
