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
#include <stdint.h>

#include "autil/mem_pool/Pool.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/util/DeVirtual.h"

namespace indexlib { namespace util {

#pragma pack(push, 1)
struct ByteSlice {
public:
    ByteSlice() noexcept = default;
    bool operator==(const ByteSlice& other) const noexcept
    {
        return next == other.next && data == other.data && size == other.size && dataSize == other.dataSize &&
               offset == other.offset;
    }

    static constexpr size_t GetHeadSize() noexcept;

    /*dataSize is the size of the data*/
    static ByteSlice* CreateObject(size_t dataSize, autil::mem_pool::Pool* pool = NULL) noexcept;

    static void DestroyObject(ByteSlice* byteSlice, autil::mem_pool::Pool* pool = NULL) noexcept;

    static ByteSlice* GetImmutableEmptyObject() noexcept
    {
        static ByteSlice slice;
        return &slice;
    }

public:
    // TODO: test perf for volatile in lookup
    ByteSlice* volatile next = nullptr;
    uint8_t* volatile data = nullptr;
    size_t volatile size = 0;

    // for BlockByteSliceList
    size_t volatile dataSize = 0; // actual block data size attached
    size_t volatile offset = 0;   // offset of this slice in underlying file, needed by BlockByteSliceList
    // uint32_t volatile idx; // index of this slice in ByteSliceList, needed by BlockByteSlice
};

#pragma pack(pop)

IE_BASE_DECLARE_SUB_CLASS_BEGIN(ByteSliceList)
IE_BASE_DECLARE_SUB_CLASS(ByteSliceList, BlockByteSliceList)
IE_BASE_DECLARE_SUB_CLASS_END(ByteSliceList)

class ByteSliceList
{
public:
    ByteSliceList() noexcept;
    ByteSliceList(ByteSlice* slice) noexcept;
    virtual ~ByteSliceList() noexcept;

public:
    void Add(ByteSlice* slice) noexcept;
    size_t GetTotalSize() const noexcept { return _totalSize; }
    size_t UpdateTotalSize() noexcept;
    void IncrementTotalSize(size_t incValue) noexcept { _totalSize = _totalSize + incValue; }
    ByteSlice* GetHead() const noexcept { return _head; }
    ByteSlice* GetTail() const noexcept { return _tail; }

    void MergeWith(ByteSliceList& other) noexcept;
    virtual void Clear(autil::mem_pool::Pool* pool) noexcept;
    virtual future_lite::coro::Lazy<bool> Prefetch(size_t length) noexcept { co_return true; }

    IE_BASE_CLASS_DECLARE(ByteSliceList);

protected:
    ByteSlice* _head;
    ByteSlice* _tail;
    size_t volatile _totalSize;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ByteSliceList> ByteSliceListPtr;

inline constexpr size_t ByteSlice::GetHeadSize() noexcept
{
    //    return sizeof(ByteSlice*) + sizeof(uint8_t*) + sizeof(uint32_t) + sizeof(uint32_t);
    return sizeof(ByteSlice);
}

/*dataSize is the size of the data*/
inline ByteSlice* ByteSlice::CreateObject(size_t dataSize, autil::mem_pool::Pool* pool) noexcept
{
    uint8_t* mem;
    size_t memSize = dataSize + GetHeadSize();
    if (pool == NULL) {
        mem = new uint8_t[memSize];
    } else {
        mem = (uint8_t*)pool->allocate(memSize);
    }
    ByteSlice* byteSlice = new (mem) ByteSlice;
    byteSlice->data = mem + GetHeadSize();
    byteSlice->size = dataSize;
    byteSlice->dataSize = 0;
    byteSlice->offset = 0;
    return byteSlice;
}

inline void ByteSlice::DestroyObject(ByteSlice* byteSlice, autil::mem_pool::Pool* pool) noexcept
{
    uint8_t* mem = (uint8_t*)byteSlice;
    if (pool == NULL) {
        delete[] mem;
    } else {
        pool->deallocate(mem, byteSlice->size + GetHeadSize());
    }
}
}} // namespace indexlib::util
