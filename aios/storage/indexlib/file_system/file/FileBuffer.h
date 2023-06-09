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
#include <stdint.h>
#include <string.h>

#include "autil/Lock.h"
#include "autil/Log.h"

namespace indexlib { namespace file_system {

class FileBuffer
{
public:
    FileBuffer(uint32_t bufferSize) noexcept;
    ~FileBuffer() noexcept;

public:
    void Wait() noexcept;
    void Notify() noexcept;

public:
    char* GetBaseAddr() const noexcept { return _buffer; }
    uint32_t GetCursor() const noexcept { return _cursor; }
    void SetCursor(uint32_t cursor) noexcept { _cursor = cursor; }
    uint32_t GetBufferSize() const noexcept { return _bufferSize; }
    size_t GetFreeSpace() const noexcept { return _bufferSize - _cursor; }
    void SetBusy() noexcept { _busy = true; }

public:
    // for write
    void CopyToBuffer(const char* src, uint32_t len) noexcept
    {
        assert(len + _cursor <= _bufferSize);
        memcpy(_buffer + _cursor, src, len);
        _cursor += len;
    }

protected:
    char* _buffer;
    uint32_t _cursor;
    uint32_t _bufferSize;
    volatile bool _busy;
    autil::ThreadCond _cond;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileBuffer> FileBufferPtr;
}} // namespace indexlib::file_system
