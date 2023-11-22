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
#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {

template <typename T>
class ReferenceCompressIntReader
{
public:
    ReferenceCompressIntReader() : _data(NULL), _first(0), _base(0), _cursor(0), _len(0) {}
    ~ReferenceCompressIntReader() {}

public:
    void Reset(char* buffer)
    {
        int32_t header = *(int32_t*)buffer;
        if (header < 0) {
            header = -header;
            _len = header >> 16;
            _base = *(T*)(buffer + sizeof(int32_t));
            _first = _base;
            _data = buffer + sizeof(int32_t) + sizeof(T);
            _cursor = 1;
        } else {
            _len = sizeof(T);
            _first = *(T*)buffer;
            _base = 0;
            _data = buffer + sizeof(T);
            _cursor = 1;
        }
    }
    template <typename ElementType>
    inline T DoSeek(T value)
    {
        T curValue = T();
        assert(value > _base);
        T compValue = value - _base;
        uint32_t cursor = _cursor - 1;
        do {
            curValue = ((ElementType*)_data)[cursor++];
        } while (curValue < compValue);
        _cursor = cursor + 1;
        return curValue + _base;
    }

    T Seek(T value)
    {
        switch (_len) {
        case 1:
            return DoSeek<uint8_t>(value);
        case 2:
            return DoSeek<uint16_t>(value);
        case 4:
            return DoSeek<uint32_t>(value);
        default:
            assert(false);
        }
        return T();
    }
    T operator[](size_t index) const
    {
        if (index == 0) {
            return _first;
        }
        char* start = _data + _len * (index - 1);
        switch (_len) {
        case 1:
            return *(uint8_t*)start + _base;
        case 2:
            return *(uint16_t*)start + _base;
        case 4:
            return *(uint32_t*)start + _base;
        default:
            assert(false);
        }
        return T();
    }
    uint32_t GetCursor() const { return _cursor; }

private:
    char* _data;
    T _first;
    T _base;
    uint32_t _cursor;
    uint8_t _len;
};

template <typename T>
class ReferenceCompressIntEncoder : public IntEncoder<T>
{
public:
    ReferenceCompressIntEncoder() {}
    ~ReferenceCompressIntEncoder() {}

public:
    std::pair<Status, uint32_t> Encode(file_system::ByteSliceWriter& sliceWriter, const T* src,
                                       uint32_t srcLen) const override;
    std::pair<Status, uint32_t> Decode(T* dest, uint32_t destLen,
                                       file_system::ByteSliceReader& sliceReader) const override;
    std::pair<Status, uint32_t> DecodeMayCopy(T*& dest, uint32_t destLen,
                                              file_system::ByteSliceReader& sliceReader) const override;

public:
    static T GetFirstValue(char* buffer);

private:
    uint8_t CalculateLen(size_t maxDelta) const;
    void DecodeHeader(int32_t header, uint32_t& compressedLen, uint32_t& count) const;

private:
    static const uint32_t MAX_ITEM_COUNT = MAX_DOC_PER_RECORD;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, ReferenceCompressIntEncoder, T);

template <typename T>
std::pair<Status, uint32_t> ReferenceCompressIntEncoder<T>::Encode(file_system::ByteSliceWriter& sliceWriter,
                                                                   const T* src, uint32_t srcLen) const
{
    assert(srcLen);
    T base = src[0];
    size_t maxDelta = (size_t)(src[srcLen - 1] - base);
    uint8_t len = CalculateLen(maxDelta);
    if (len == sizeof(T) && srcLen == MAX_ITEM_COUNT) {
        // do not compress
        sliceWriter.Write((const void*)src, srcLen * sizeof(T));
        return std::make_pair(Status::OK(), srcLen * sizeof(T));
    }
    int32_t header = 0;
    header += len << 16;
    header += srcLen & 0xffff;
    header = -header;
    sliceWriter.Write(header);
    sliceWriter.Write(base);
    for (size_t i = 1; i < srcLen; ++i) {
        switch (len) {
        case 1:
            sliceWriter.Write((uint8_t)(src[i] - base));
            break;
        case 2:
            sliceWriter.Write((uint16_t)(src[i] - base));
            break;
        case 4:
            sliceWriter.Write((uint32_t)(src[i] - base));
            break;
        case 8:
            sliceWriter.Write((uint64_t)(src[i] - base));
            break;
        default:
            assert(false);
        }
    }
    return std::make_pair(Status::OK(), sizeof(int32_t) + sizeof(T) + (srcLen - 1) * len);
}

template <typename T>
std::pair<Status, uint32_t> ReferenceCompressIntEncoder<T>::Decode(T* dest, uint32_t destLen,
                                                                   file_system::ByteSliceReader& sliceReader) const
{
    auto peekRet = sliceReader.PeekInt32();
    RETURN_RESULT_IF_FS_ERROR(peekRet.Code(), std::make_pair(peekRet.Status(), 0), "PeekInt32 failed");
    int32_t header = peekRet.Value();
    uint32_t compressedLen = 0;
    uint32_t count = 0;
    DecodeHeader(header, compressedLen, count);
    auto ret = sliceReader.Read((void*)dest, compressedLen);
    RETURN_RESULT_IF_FS_ERROR(ret.Code(), std::make_pair(ret.Status(), uint32_t(0)), "Read failed");
    uint32_t actualLen = ret.Value();
    if (actualLen != compressedLen) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), 0, "Decode posting FAILED.");
    }
    return std::make_pair(Status::OK(), count);
}

template <typename T>
std::pair<Status, uint32_t>
ReferenceCompressIntEncoder<T>::DecodeMayCopy(T*& dest, uint32_t destLen,
                                              file_system::ByteSliceReader& sliceReader) const
{
    auto peekRet = sliceReader.PeekInt32();
    RETURN_RESULT_IF_FS_ERROR(peekRet.Code(), std::make_pair(peekRet.Status(), 0), "PeekInt32 failed");
    int32_t header = peekRet.Value();
    uint32_t compressedLen = 0;
    uint32_t count = 0;
    DecodeHeader(header, compressedLen, count);
    auto ret = sliceReader.ReadMayCopy((void*&)dest, compressedLen);
    RETURN_RESULT_IF_FS_ERROR(ret.Code(), std::make_pair(ret.Status(), uint32_t(0)), "ReadMayCopy failed");
    uint32_t actualLen = ret.Value();
    if (actualLen != compressedLen) {
        RETURN2_IF_STATUS_ERROR(Status::Corruption(), 0, "Decode posting FAILED.");
    }
    return std::make_pair(Status::OK(), count);
}

template <typename T>
inline T ReferenceCompressIntEncoder<T>::GetFirstValue(char* buffer)
{
    ReferenceCompressIntReader<T> reader;
    reader.Reset(buffer);
    return reader[0];
}

template <typename T>
uint8_t ReferenceCompressIntEncoder<T>::CalculateLen(size_t maxDelta) const
{
    if (maxDelta <= (size_t)0xff) {
        return 1;
    } else if (maxDelta <= (size_t)0xffff) {
        return 2;
    }
    return 4;
}

template <typename T>
void ReferenceCompressIntEncoder<T>::DecodeHeader(int32_t header, uint32_t& compressedLen, uint32_t& count) const
{
    if (header < 0) {
        header = -header;
        size_t len = header >> 16;
        count = header & 0xffff;
        compressedLen = sizeof(int32_t) + sizeof(T) + (count - 1) * len;
    } else {
        compressedLen = sizeof(T) * MAX_ITEM_COUNT;
        count = MAX_ITEM_COUNT;
    }
}

#define REFERENCE_COMPRESS_INTENCODER(type)                                                                            \
    typedef ReferenceCompressIntEncoder<uint##type##_t> ReferenceCompressInt##type##Encoder;

REFERENCE_COMPRESS_INTENCODER(8);
REFERENCE_COMPRESS_INTENCODER(16);
REFERENCE_COMPRESS_INTENCODER(32);
} // namespace indexlib::index
