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

template <typename T>
class IntEncoder
{
public:
    IntEncoder() {}
    virtual ~IntEncoder() {}

public:
    /**
     * Encode T array to byte slice list
     * @param sliceWriter where encoded bytes stored
     * @param src source T array
     * @param srcLen length of /src/
     * @return length of encoded byte array
     */
    virtual std::pair<Status, uint32_t> Encode(file_system::ByteSliceWriter& sliceWriter, const T* src,
                                               uint32_t srcLen) const = 0;
    virtual std::pair<Status, uint32_t> Encode(uint8_t* dest, const T* src, uint32_t srcLen) const
    {
        assert(false);
        return {};
    }

    /**
     * Decode encoded bytes to T array
     * @param dest where decoded T values stored
     * @param destLen length of /dest/
     * @param sliceReader where encoded bytes read from
     * @return count of decoded T array
     */
    virtual std::pair<Status, uint32_t> Decode(T* dest, uint32_t destLen,
                                               file_system::ByteSliceReader& sliceReader) const = 0;

    virtual std::pair<Status, uint32_t> DecodeMayCopy(T*& dest, uint32_t destLen,
                                                      file_system::ByteSliceReader& sliceReader) const
    {
        return Decode(dest, destLen, sliceReader);
    }

private:
    AUTIL_LOG_DECLARE();
};

#define INTENCODER(type) typedef IntEncoder<uint##type##_t> Int##type##Encoder;

INTENCODER(8);
INTENCODER(16);
INTENCODER(32);
} // namespace indexlib::index
