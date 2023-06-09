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

#include "indexlib/index/common/numeric_compress/IntEncoder.h"

namespace indexlib::index {

template <typename T>
class NoCompressIntEncoder : public IntEncoder<T>
{
public:
    NoCompressIntEncoder(bool hasLength = true) : _hasLength(hasLength) {}
    ~NoCompressIntEncoder() {}

public:
    std::pair<Status, uint32_t> Encode(indexlib::file_system::ByteSliceWriter& sliceWriter, const T* src,
                                       uint32_t srcLen) const override;
    std::pair<Status, uint32_t> Decode(T* dest, uint32_t destLen,
                                       indexlib::file_system::ByteSliceReader& sliceReader) const override;
    std::pair<Status, uint32_t> DecodeMayCopy(T*& dest, uint32_t destLen,
                                              indexlib::file_system::ByteSliceReader& sliceReader) const override;

private:
    bool _hasLength;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
std::pair<Status, uint32_t> NoCompressIntEncoder<T>::Encode(indexlib::file_system::ByteSliceWriter& sliceWriter,
                                                            const T* src, uint32_t srcLen) const
{
    // src len using uint8_t, because MAX_RECORD_SIZE is 128
    uint32_t encodeLen = 0;
    if (_hasLength) {
        sliceWriter.WriteByte((uint8_t)srcLen);
        encodeLen += sizeof(uint8_t);
    }
    sliceWriter.Write((const void*)src, srcLen * sizeof(T));
    return std::make_pair(Status::OK(), encodeLen + srcLen * sizeof(T));
}

template <typename T>
std::pair<Status, uint32_t> NoCompressIntEncoder<T>::Decode(T* dest, uint32_t destLen,
                                                            indexlib::file_system::ByteSliceReader& sliceReader) const
{
    uint32_t readCount = 0;
    if (_hasLength) {
        readCount = sliceReader.ReadByte();
    } else {
        readCount = destLen;
    }
    uint32_t actualLen = sliceReader.Read((void*)dest, readCount * sizeof(T));
    assert(_hasLength || actualLen / sizeof(T) == destLen);
    (void)actualLen;
    return std::make_pair(Status::OK(), readCount);
}

template <typename T>
std::pair<Status, uint32_t>
NoCompressIntEncoder<T>::DecodeMayCopy(T*& dest, uint32_t destLen,
                                       indexlib::file_system::ByteSliceReader& sliceReader) const
{
    uint32_t readCount = 0;
    if (_hasLength) {
        readCount = sliceReader.ReadByte();
    } else {
        readCount = destLen;
    }
    uint32_t actualLen = sliceReader.ReadMayCopy((void*&)dest, readCount * sizeof(T));
    assert(_hasLength || actualLen / sizeof(T) == destLen);
    (void)actualLen;
    return std::make_pair(Status::OK(), readCount);
}

#define NO_COMPRESS_INTENCODER(type) typedef NoCompressIntEncoder<uint##type##_t> NoCompressInt##type##Encoder;

NO_COMPRESS_INTENCODER(8);
NO_COMPRESS_INTENCODER(16);
NO_COMPRESS_INTENCODER(32);
} // namespace indexlib::index
