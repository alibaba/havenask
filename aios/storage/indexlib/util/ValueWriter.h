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

#include "autil/ConstString.h"
#include "indexlib/base/Define.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace util {

template <typename ValueOffsetType>
class ValueWriter
{
public:
    explicit ValueWriter(bool enableRewrite = false)
        : _reserveBytes(0)
        , _usedBytes(0)
        , _baseAddr(NULL)
        , _enableRewrite(enableRewrite)
    {
    }
    ~ValueWriter() {}

    void Init(char* baseAddr, ValueOffsetType reserveBytes)
    {
        assert(baseAddr);

        _reserveBytes = reserveBytes;
        _baseAddr = baseAddr;
    }

    char* GetValue(ValueOffsetType offset)
    {
        assert(offset < _usedBytes);
        return _baseAddr + offset;
    }

    const char* GetValue(ValueOffsetType offset) const
    {
        assert(offset < _usedBytes);
        return _baseAddr + offset;
    }

    const char* GetBaseAddress() const { return _baseAddr; }
    ValueOffsetType GetUsedBytes() const { return _usedBytes; }
    ValueOffsetType GetReserveBytes() const { return _reserveBytes; }

public:
    ValueOffsetType Append(const autil::StringView& value) { return Append(value.data(), value.size()); }

    ValueOffsetType Append(const char* data, size_t size)
    {
        if (_usedBytes + size > _reserveBytes) {
            INDEXLIB_FATAL_ERROR(OutOfMemory,
                                 "value writer exceed mem reserve, reserve size [%lu], used bytes [%lu]"
                                 ", value size [%lu]",
                                 (size_t)_reserveBytes, (size_t)_usedBytes, size);
        }
        char* addr = _baseAddr + _usedBytes;
        memcpy(addr, data, size);
        ValueOffsetType ret = _usedBytes;
        MEMORY_BARRIER();
        _usedBytes += size;
        return ret;
    }

    void Rewrite(const autil::StringView& value, ValueOffsetType offset)
    {
        if (!_enableRewrite) {
            INDEXLIB_FATAL_ERROR(UnSupported, "value rewrite is disabled");
        }

        if (offset + value.size() > _usedBytes) {
            INDEXLIB_FATAL_ERROR(OutOfRange, "value offset [%lu] out of range, value size [%lu], used bytes [%lu]",
                                 (size_t)offset, value.size(), (size_t)_usedBytes);
        }
        char* addr = _baseAddr + offset;
        memcpy(addr, value.data(), value.size());
    }

private:
    ValueOffsetType _reserveBytes;
    ValueOffsetType _usedBytes;
    char* _baseAddr;
    bool _enableRewrite;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP_TEMPLATE(indexlib.util, ValueWriter, ValueOffsetType);
}} // namespace indexlib::util
