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

#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/AtomicValueTyped.h"
#include "indexlib/index/common/MultiValue.h"

namespace indexlib::index {

/*
 * NOTICE:
 * max row count: 8; max column count: 128
 * column allocate strategy: 2->16->128
 */
class ShortBuffer
{
public:
    typedef autil::mem_pool::Pool Pool;

public:
    // only support max row count is 8
    ShortBuffer(Pool* pool = NULL);
    ~ShortBuffer();

public:
    void Init(const MultiValue* multiValue)
    {
        _multiValue = multiValue;
        const AtomicValueVector& values = multiValue->GetAtomicValues();
        for (size_t i = 0; i < values.size(); ++i) {
            _offset[i] = values[i]->GetOffset();
        }
    }

    // return false, when push back to the same row repeatedly
    //                    and failed to allocate memory
    template <typename T>
    bool PushBack(uint8_t row, T value);

    void EndPushBack() { _size = _size + 1; }

    uint8_t Capacity() const { return _capacity; }
    uint8_t Size() const { return _size; }

    uint8_t GetRowCount() const { return _multiValue->GetAtomicValueSize(); }

    template <typename T>
    T* GetRowTyped(uint8_t n);

    template <typename T>
    const T* GetRowTyped(uint8_t n) const;

    uint8_t* GetRow(uint8_t n);

    template <typename T>
    T* operator[](uint8_t row)
    {
        return (T*)GetRow(row);
    }

    void Clear();
    bool SnapShot(ShortBuffer& buffer) const;

    Pool* GetPool() const { return _pool; }

    const MultiValue* GetMultiValue() const { return _multiValue; }

private:
    void* Allocate(size_t size) { return _pool->allocate(size); }
    void Deallocate(void* buf, size_t size) { _pool->deallocate(buf, size); }

    bool Reallocate();
    static uint8_t AllocatePlan(uint8_t curCapacity);

    static void BufferMemoryCopy(uint8_t* dst, uint8_t dstColCount, uint8_t* src, uint8_t srcColCount,
                                 const MultiValue* multiValue, uint8_t srcSize);

    bool IsFull() const { return _size >= _capacity; }

    static uint8_t* GetRow(uint8_t* buffer, uint8_t capacity, const AtomicValue* atomicValue);

    void ReleaseBuffer(uint8_t* buffer, uint8_t capacity);
    void Reserve(uint8_t size);

protected:
    uint8_t* volatile _buffer;
    uint8_t _offset[8];
    uint8_t volatile _capacity;
    uint8_t volatile _size;

    bool volatile _isBufferValid;
    bool _hasPool;
    Pool* _pool;
    const MultiValue* _multiValue;

private:
    AUTIL_LOG_DECLARE();
    friend class ShortBufferTest;
};

////////////////////////////////////////////////////////////////////////

template <typename T>
inline bool ShortBuffer::PushBack(uint8_t row, T value)
{
    if (IsFull()) {
        if (!Reallocate()) {
            return false;
        }
    }
    assert(ValueTypeTraits<T>::TYPE == _multiValue->GetAtomicValue(row)->GetType());
    T* rowData = GetRowTyped<T>(row);
    if (!rowData) {
        return false;
    }
    rowData[_size] = value;

    return true;
}

template <typename T>
inline T* ShortBuffer::GetRowTyped(uint8_t n)
{
    assert(n < GetRowCount());
    return (T*)(_buffer + _offset[n] * _capacity);
}

template <typename T>
inline const T* ShortBuffer::GetRowTyped(uint8_t n) const
{
    assert(n < GetRowCount());
    return (const T*)(_buffer + _offset[n] * _capacity);
}

inline uint8_t* ShortBuffer::GetRow(uint8_t n)
{
    AtomicValue* atomicValue = _multiValue->GetAtomicValue(n);
    return GetRow((uint8_t*)_buffer, _capacity, atomicValue);
}

inline uint8_t* ShortBuffer::GetRow(uint8_t* buffer, uint8_t capacity, const AtomicValue* atomicValue)
{
    uint32_t offset = atomicValue->GetOffset();
    if (!offset) {
        return buffer;
    }
    return buffer + offset * capacity;
}

} // namespace indexlib::index
