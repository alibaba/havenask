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
#include <stddef.h>
#include <stdint.h>

#include "autil/Log.h"
#include "indexlib/file_system/file/FileWriterImpl.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace file_system {

class InterimFileWriter : public FileWriterImpl
{
public:
    InterimFileWriter() noexcept;
    ~InterimFileWriter() noexcept;

public:
    ErrorCode DoOpen() noexcept override { return FSEC_OK; }
    FSResult<size_t> Write(const void* buffer, size_t length) noexcept override;
    size_t GetLength() const noexcept override { return _cursor; }
    ErrorCode DoClose() noexcept override { return FSEC_OK; }
    FSResult<void> ReserveFile(size_t reserveSize) noexcept override
    {
        assert(false);
        return FSEC_OK;
    }
    FSResult<void> Truncate(size_t truncateSize) noexcept override
    {
        assert(false);
        return FSEC_OK;
    }
    void* GetBaseAddress() noexcept override { return _buf; }

public:
    void Init(uint64_t length) noexcept;
    // FileWriter owned buffer, so it'll be released when FileWriter released
    // write will cause ByteSliceListPtr invalid
    util::ByteSliceListPtr GetByteSliceList(bool isVIntHeader) const noexcept;
    // write will cause BaseAddress invalid

private:
    size_t CalculateSize(size_t needLength) noexcept;
    void Extend(size_t extendSize) noexcept;

public:
    // for ut
    util::ByteSliceListPtr CopyToByteSliceList(bool isVIntHeader) noexcept;

private:
    uint32_t ReadVUInt32(char*& cursor) const noexcept
    {
        uint8_t byte = *(uint8_t*)cursor++;
        uint32_t value = byte & 0x7F;
        int shift = 7;

        while (byte & 0x80) {
            byte = *(uint8_t*)cursor++;
            value |= ((byte & 0x7F) << shift);
            shift += 7;
        }
        return value;
    }

private:
    uint8_t* _buf;
    uint64_t _length;
    uint64_t _cursor;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<InterimFileWriter> InterimFileWriterPtr;
}} // namespace indexlib::file_system
