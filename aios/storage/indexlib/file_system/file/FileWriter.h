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

#include <functional>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/WriterOption.h"

namespace indexlib { namespace file_system {

class FileWriter : public autil::NoCopyable
{
public:
    FileWriter() noexcept = default;
    virtual ~FileWriter() noexcept = default;
    virtual FSResult<void> Open(const std::string& logicalPath, const std::string& physicalPath) noexcept = 0;
    virtual FSResult<void> Close() noexcept = 0;
    virtual const std::string& GetLogicalPath() const noexcept = 0;
    virtual const std::string& GetPhysicalPath() const noexcept = 0;
    virtual const WriterOption& GetWriterOption() const noexcept = 0;

public:
    virtual FSResult<size_t> Write(const void* buffer, size_t length) noexcept = 0;
    // ReserveFile does not change size, reserve memory for address access mode
    virtual FSResult<void> ReserveFile(size_t reserveSize) noexcept = 0;
    // Truncate will change the size, means resize
    virtual FSResult<void> Truncate(size_t truncateSize) noexcept = 0;
    virtual void* GetBaseAddress() noexcept = 0;
    virtual size_t GetLength() const noexcept = 0;
    // logic length match with inner offset, use uncompress file length for compresse file
    virtual size_t GetLogicLength() const noexcept { return GetLength(); }

public:
    FSResult<void> WriteVUInt32(uint32_t value) noexcept;

public:
    std::string DebugString() const noexcept { return GetLogicalPath(); }

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FileWriter> FileWriterPtr;

//////////////////////////////////////////////////////////////////////
inline FSResult<void> FileWriter::WriteVUInt32(uint32_t value) noexcept
{
    char buf[5];
    int cursor = 0;
    while (value > 0x7F) {
        buf[cursor++] = 0x80 | (value & 0x7F);
        value >>= 7;
    }
    buf[cursor++] = value & 0x7F;
    auto [ec, len] = Write((const void*)buf, cursor);
    assert((int64_t)len == cursor);
    return ec;
}
}} // namespace indexlib::file_system
