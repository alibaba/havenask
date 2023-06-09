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

#include "indexlib/file_system/ByteSliceReader.h"
#include "indexlib/file_system/ByteSliceWriter.h"

namespace indexlib::index {

class AtomicValue
{
public:
    enum ValueType {
        vt_unknown,
        vt_uint8,
        vt_uint16,
        vt_uint32,
        vt_int8,
        vt_int16,
        vt_int32,
    };

    AtomicValue()
    {
        _location = 0;
        _offset = 0;
    }
    virtual ~AtomicValue() {}

public:
    virtual ValueType GetType() const = 0;
    virtual size_t GetSize() const = 0;

    uint32_t GetLocation() const { return _location; }
    void SetLocation(const uint32_t& location) { _location = location; }

    uint32_t GetOffset() const { return _offset; }
    void SetOffset(const uint32_t& offset) { _offset = offset; }

    virtual uint32_t Encode(uint8_t mode, file_system::ByteSliceWriter& sliceWriter, const uint8_t* src,
                            uint32_t len) const = 0;

    virtual uint32_t Decode(uint8_t mode, uint8_t* dest, uint32_t destLen,
                            file_system::ByteSliceReader& sliceReader) const = 0;

private:
    uint32_t _location;
    uint32_t _offset;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::vector<AtomicValue*> AtomicValueVector;
} // namespace indexlib::index
