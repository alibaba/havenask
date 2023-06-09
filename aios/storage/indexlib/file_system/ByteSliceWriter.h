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
#include <new>
#include <stddef.h>
#include <stdint.h>

#include "autil/Log.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"

namespace indexlib { namespace file_system {

class ByteSliceWriter
{
public:
    constexpr static uint32_t MIN_SLICE_SIZE = util::ByteSlice::GetHeadSize();

public:
    ByteSliceWriter(autil::mem_pool::Pool* pool = NULL, uint32_t minSliceSize = MIN_SLICE_SIZE) noexcept;
    ~ByteSliceWriter() noexcept;

public:
    void WriteByte(uint8_t value) noexcept;
    void WriteInt16(int16_t value) noexcept;
    void WriteInt32(int32_t value) noexcept;
    void WriteUInt32(uint32_t value) noexcept;
    void Write(const void* value, size_t len) noexcept;
    void Write(util::ByteSliceList& src) noexcept;

    template <typename T>
    void Write(T value) noexcept;

    /**
     * copy data from src[start, end) to *this
     */
    void Write(const util::ByteSliceList& src, uint32_t start, uint32_t end) noexcept(false);
    uint32_t WriteVInt(uint32_t value) noexcept;

    /**
     * hold an int32 slot for later value updating
     */
    int32_t& HoldInt32() noexcept;

    size_t GetSize() const noexcept;
    util::ByteSliceList* GetByteSliceList() noexcept { return _sliceList; }

    const util::ByteSliceList* GetByteSliceList() const noexcept { return _sliceList; }

    FSResult<void> Dump(const std::shared_ptr<FileWriter>& file) noexcept;

    // snapshot writer can't call this interface
    void Reset() noexcept;
    void Close() noexcept;

    // shallow copy, not own byte slice list
    void SnapShot(ByteSliceWriter& byteSliceWriter) const noexcept;

#ifdef PACK_INDEX_MEMORY_STAT
    uint32_t GetAllocatedSize() const noexcept { return _allocatedSize; }
#endif

protected:
    // disable copy ctor
    ByteSliceWriter(const ByteSliceWriter& other) noexcept;
    ByteSliceWriter& operator=(const ByteSliceWriter& other) noexcept;

protected:
    util::ByteSlice* CreateSlice(uint32_t size) noexcept;
    uint32_t GetIncrementedSliceSize(uint32_t prevSliceSize) noexcept;

    template <typename T>
    util::ByteSlice* GetSliceForWrite() noexcept;

    util::ByteSliceList* AllocateByteSliceList() noexcept;
    void DeAllocateByteSliceList(util::ByteSliceList*& byteSliceList) noexcept;

private:
    util::ByteSliceList* _sliceList;
    autil::mem_pool::Pool* _pool;
    uint32_t _lastSliceSize;
    bool _isOwnSliceList;

#ifdef PACK_INDEX_MEMORY_STAT
    uint32_t _allocatedSize;
#endif

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ByteSliceWriter> ByteSliceWriterPtr;

//////////////////////////////////////////////////////////////
//
inline void ByteSliceWriter::WriteByte(uint8_t value) noexcept
{
    util::ByteSlice* slice = GetSliceForWrite<uint8_t>();
    slice->data[slice->size] = value;
    slice->size = slice->size + 1;
    _sliceList->IncrementTotalSize(1);
}

inline void ByteSliceWriter::WriteInt16(int16_t value) noexcept
{
    util::ByteSlice* slice = GetSliceForWrite<int16_t>();
    *(int16_t*)(slice->data + slice->size) = value;
    slice->size = slice->size + sizeof(int16_t);
    _sliceList->IncrementTotalSize(sizeof(int16_t));
}

inline void ByteSliceWriter::WriteInt32(int32_t value) noexcept
{
    util::ByteSlice* slice = GetSliceForWrite<int32_t>();
    *(int32_t*)(slice->data + slice->size) = value;
    slice->size = slice->size + sizeof(int32_t);
    _sliceList->IncrementTotalSize(sizeof(int32_t));
}

template <typename T>
inline void ByteSliceWriter::Write(T value) noexcept
{
    util::ByteSlice* slice = GetSliceForWrite<T>();
    *(T*)(slice->data + slice->size) = value;
    slice->size = slice->size + sizeof(T);
    _sliceList->IncrementTotalSize(sizeof(T));
}

inline uint32_t ByteSliceWriter::WriteVInt(uint32_t value) noexcept
{
    uint32_t len = 1;
    while (value > 0x7F) {
        WriteByte(0x80 | (value & 0x7F));
        value >>= 7;
        ++len;
    }

    WriteByte(value & 0x7F);
    return len;
}

inline void ByteSliceWriter::WriteUInt32(uint32_t value) noexcept
{
    util::ByteSlice* slice = GetSliceForWrite<uint32_t>();
    *(uint32_t*)(slice->data + slice->size) = value;
    slice->size = slice->size + sizeof(uint32_t);
    _sliceList->IncrementTotalSize(sizeof(uint32_t));
}

inline int32_t& ByteSliceWriter::HoldInt32() noexcept
{
    util::ByteSlice* slice = GetSliceForWrite<int32_t>();

    int32_t& ret = *(int32_t*)(slice->data + slice->size);
    slice->size = slice->size + sizeof(int32_t);
    _sliceList->IncrementTotalSize(sizeof(int32_t));
    return ret;
}

inline uint32_t ByteSliceWriter::GetIncrementedSliceSize(uint32_t lastSliceSize) noexcept
{
    uint32_t prev = lastSliceSize + util::ByteSlice::GetHeadSize();
    uint32_t sliceSize = prev + (prev >> 2) - util::ByteSlice::GetHeadSize();
    uint32_t chunkSize = indexlibv2::DEFAULT_CHUNK_SIZE_BYTES - util::ByteSlice::GetHeadSize() -
                         sizeof(autil::mem_pool::ChainedMemoryChunk);
    return sliceSize < chunkSize ? sliceSize : chunkSize;
}

template <typename T>
inline util::ByteSlice* ByteSliceWriter::GetSliceForWrite() noexcept
{
    util::ByteSlice* slice = _sliceList->GetTail();
    if (slice->size + sizeof(T) > _lastSliceSize) {
        _lastSliceSize = GetIncrementedSliceSize(_lastSliceSize);
        slice = CreateSlice(_lastSliceSize);
        _sliceList->Add(slice);
    }
    return slice;
}

inline util::ByteSliceList* ByteSliceWriter::AllocateByteSliceList() noexcept
{
    if (_pool) {
        void* buffer = _pool->allocate(sizeof(util::ByteSliceList));
        return new (buffer) util::ByteSliceList();
    } else {
        return new util::ByteSliceList();
    }
}

inline void ByteSliceWriter::DeAllocateByteSliceList(util::ByteSliceList*& byteSliceList) noexcept
{
    if (_pool) {
        byteSliceList->~ByteSliceList();
        byteSliceList = NULL;
    } else {
        delete byteSliceList;
        byteSliceList = NULL;
    }
}
}} // namespace indexlib::file_system
