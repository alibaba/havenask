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
#include <string>

#include "indexlib/file_system/file/BlockByteSliceList.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace file_system {
class ByteSliceReader
{
public:
    static const int BYTE_SLICE_EOF = -1;

public:
    ByteSliceReader();
    ByteSliceReader(util::ByteSliceList* sliceList);

public:
    uint8_t ReadByte();
    int16_t ReadInt16();
    int32_t ReadInt32();
    uint32_t ReadUInt32();
    int64_t ReadInt64();
    uint64_t ReadUInt64();
    int32_t ReadVInt32();
    uint32_t ReadVUInt32();

    size_t Read(void* value, size_t len);

    /*
     * Parameter value may be modified, so you should call the function
     * with a temporary pointer to the destination buffer.
     */
    size_t ReadMayCopy(void*& value, size_t len);

    int32_t PeekInt32();

    size_t Seek(size_t offset);
    size_t Tell() const { return _globalOffset; }
    bool CurrentSliceEnough(size_t len) { return _currentSliceOffset + len <= GetSliceDataSize(_currentSlice); }
    uint8_t* GetCurrentSliceData() { return _currentSlice->data + _currentSliceOffset; }

public:
    void Open(util::ByteSliceList* sliceList);
    void Open(util::ByteSlice* slice);
    void Close();
    size_t GetSize() const { return _sliceList->GetTotalSize(); }

    util::ByteSliceList* GetByteSliceList() const { return _sliceList; }
    bool End() const { return Tell() + 1 >= _size; }

private:
    void SetSlice(util::ByteSlice* slice);

private:
    template <typename T>
    inline T ReadInt();

    void HandleError(const std::string& msg) const;
    inline util::ByteSlice* NextSlice(util::ByteSlice* byteSlice);
    inline size_t GetSliceDataSize(util::ByteSlice* byteSlice) const;
    inline util::ByteSlice* PrepareSlice(util::ByteSlice* byteSlice);

protected:
    util::ByteSlice* _currentSlice;
    size_t _currentSliceOffset;
    size_t _globalOffset;
    util::ByteSliceList* _sliceList;
    size_t _size;
    bool _isBlockByteSliceList;
};

typedef std::shared_ptr<ByteSliceReader> ByteSliceReaderPtr;

//////////////////////////////////////////////////////////////
//
inline uint8_t ByteSliceReader::ReadByte()
{
    if (_currentSliceOffset >= GetSliceDataSize(_currentSlice)) {
        _currentSlice = NextSlice(_currentSlice);
        if (!_currentSlice) {
            HandleError("Read past EOF.");
        }
        _currentSliceOffset = 0;
    }
    uint8_t value = _currentSlice->data[_currentSliceOffset++];
    _globalOffset++;
    return value;
}

inline int16_t ByteSliceReader::ReadInt16() { return ReadInt<int16_t>(); }

inline int32_t ByteSliceReader::ReadInt32() { return ReadInt<int32_t>(); }

inline uint32_t ByteSliceReader::ReadUInt32() { return ReadInt<uint32_t>(); }

inline int32_t ByteSliceReader::ReadVInt32() { return (int32_t)ReadVUInt32(); }

inline uint32_t ByteSliceReader::ReadVUInt32()
{
    uint8_t byte = ReadByte();
    uint32_t value = byte & 0x7F;
    int shift = 7;

    while (byte & 0x80) {
        byte = ReadByte();
        value |= ((byte & 0x7F) << shift);
        shift += 7;
    }
    return value;
}

inline int64_t ByteSliceReader::ReadInt64() { return ReadInt<int64_t>(); }

inline uint64_t ByteSliceReader::ReadUInt64() { return ReadInt<uint64_t>(); }

inline int32_t ByteSliceReader::PeekInt32()
{
    assert(_currentSlice != NULL);

    if (_currentSliceOffset + sizeof(int32_t) <= GetSliceDataSize(_currentSlice)) {
        return *((int32_t*)(_currentSlice->data + _currentSliceOffset));
    }

    uint8_t bytes[sizeof(int32_t)];
    char* buffer = (char*)bytes;
    util::ByteSlice* slice = _currentSlice;
    util::ByteSlice* nextSlice = NULL;
    size_t curSliceOff = _currentSliceOffset;
    for (size_t i = 0; i < sizeof(int32_t); ++i) {
        if (curSliceOff >= GetSliceDataSize(slice)) {
            nextSlice = NextSlice(slice);
            if (nextSlice == NULL || nextSlice->data == NULL) {
                HandleError("Read past EOF.");
            } else {
                slice = nextSlice;
            }
            curSliceOff = 0;
        }
        bytes[i] = slice->data[curSliceOff++];
    }
    return *((int32_t*)buffer);
}

template <typename T>
inline T ByteSliceReader::ReadInt()
{
    auto currentSliceDataSize = GetSliceDataSize(_currentSlice);
    if (_currentSliceOffset + sizeof(T) <= currentSliceDataSize) {
        T value = *((T*)(_currentSlice->data + _currentSliceOffset));
        _currentSliceOffset += sizeof(T);
        _globalOffset += sizeof(T);
        return value;
    }

    uint8_t bytes[sizeof(T)];
    char* buffer = (char*)bytes;
    for (size_t i = 0; i < sizeof(T); ++i) {
        bytes[i] = ReadByte();
    }
    return *((T*)buffer);
}

inline util::ByteSlice* ByteSliceReader::NextSlice(util::ByteSlice* byteSlice)
{
    if (!_isBlockByteSliceList) {
        return byteSlice->next;
    }
    auto blockSliceList = static_cast<BlockByteSliceList*>(_sliceList);
    return blockSliceList->GetNextSlice(byteSlice);
}

inline size_t ByteSliceReader::GetSliceDataSize(util::ByteSlice* byteSlice) const
{
    if (!_isBlockByteSliceList) {
        return byteSlice->size;
    }
    return byteSlice->dataSize;
}

inline util::ByteSlice* ByteSliceReader::PrepareSlice(util::ByteSlice* byteSlice)
{
    if (!_isBlockByteSliceList) {
        return byteSlice;
    }
    auto blockSliceList = static_cast<BlockByteSliceList*>(_sliceList);
    auto fileOffset = byteSlice->offset + _currentSliceOffset;
    auto newSlice = blockSliceList->GetSlice(fileOffset, byteSlice);
    _currentSliceOffset = fileOffset - newSlice->offset;
    return newSlice;
}
}} // namespace indexlib::file_system
