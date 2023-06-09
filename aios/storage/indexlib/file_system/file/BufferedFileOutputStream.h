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

#include <memory>
#include <stddef.h>
#include <string>

#include "autil/Log.h"
#include "indexlib/file_system/file/BufferedFileNode.h"
#include "indexlib/file_system/file/FileBuffer.h"

namespace indexlib { namespace file_system {

class BufferedFileOutputStream
{
public:
    BufferedFileOutputStream(bool isAppendMode) noexcept : _isAppendMode(isAppendMode) {}
    virtual ~BufferedFileOutputStream() noexcept {}

public:
    FSResult<void> Open(const std::string& path, int64_t fileLength) noexcept;
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept;
    FSResult<bool> ForceFlush() noexcept;
    virtual FSResult<void> Close() noexcept;
    size_t GetLength() const noexcept;

private:
    ErrorCode InitFileNode(int64_t fileLength) noexcept;
    FSResult<size_t> FlushBuffer() noexcept;

protected:
    // for mock
    virtual BufferedFileNode* CreateFileNode() const noexcept;

private:
    std::string _path;
    std::unique_ptr<BufferedFileNode> _fileNode;
    std::unique_ptr<FileBuffer> _buffer;
    bool _isAppendMode = false;

private:
    friend class BufferedFileWriterTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<BufferedFileOutputStream> BufferedFileOutputStreamPtr;
}} // namespace indexlib::file_system
