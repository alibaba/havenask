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
#include "indexlib/index/common/ShortBuffer.h"

#include "indexlib/base/Define.h"
#include "indexlib/index/inverted_index/Constant.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, ShortBuffer);

ShortBuffer::ShortBuffer(Pool* pool)
    : _buffer(NULL)
    , _capacity(0)
    , _size(0)
    , _isBufferValid(true)
    , _hasPool(false)
    , _pool(pool)
    , _multiValue(NULL)
{
    if (!_pool) {
        _pool = new Pool;
        _hasPool = true;
    }
}

ShortBuffer::~ShortBuffer()
{
    // here we assume this object and pool have the same life cycle
    // ReleaseBuffer((uint8_t*)_buffer, _capacity);
    if (!_hasPool) {
        // ReleaseBuffer((uint8_t*)_buffer, _capacity);
    }

    if (_hasPool) {
        DELETE_AND_SET_NULL(_pool);
    }
}

bool ShortBuffer::Reallocate()
{
    uint8_t oldCapacity = _capacity;
    uint8_t* oldBuffer = (uint8_t*)_buffer;
    uint8_t newCapacity = AllocatePlan(oldCapacity);
    if (newCapacity == oldCapacity) {
        return false;
    }
    size_t docItemSize = newCapacity * _multiValue->GetTotalSize();

    uint8_t* newBuffer = (uint8_t*)Allocate(docItemSize);
    assert(newBuffer);

    BufferMemoryCopy(newBuffer, newCapacity, (uint8_t*)_buffer, _capacity, _multiValue, _size);

    _isBufferValid = false;

    MEMORY_BARRIER();

    _buffer = newBuffer;

    MEMORY_BARRIER();

    _capacity = newCapacity;

    MEMORY_BARRIER();

    _isBufferValid = true;

    ReleaseBuffer(oldBuffer, oldCapacity);

    return true;
}

void ShortBuffer::ReleaseBuffer(uint8_t* buffer, uint8_t capacity)
{
    if (buffer == NULL || capacity == 0) {
        return;
    }
    size_t bufferSize = capacity * _multiValue->GetTotalSize();
    Deallocate((void*)buffer, bufferSize);
}

void ShortBuffer::BufferMemoryCopy(uint8_t* dst, uint8_t dstColCount, uint8_t* src, uint8_t srcColCount,
                                   const MultiValue* multiValue, uint8_t srcSize)
{
    if (src == NULL || srcSize == 0) {
        return;
    }
    for (uint8_t i = 0; i < multiValue->GetAtomicValueSize(); ++i) {
        const AtomicValue* atomicValue = multiValue->GetAtomicValue(i);
        memcpy(GetRow(dst, dstColCount, atomicValue), GetRow(src, srcColCount, atomicValue),
               srcSize * atomicValue->GetSize());
    }
}

void ShortBuffer::Clear() { _size = 0; }

bool ShortBuffer::SnapShot(ShortBuffer& shortBuffer) const
{
    shortBuffer.Clear();

    if (shortBuffer.GetRowCount() != _multiValue->GetAtomicValueSize()) {
        return false;
    }
    if (_buffer == NULL) {
        return true;
    }

    // here we assume buffer size only can be increased
    uint8_t sizeSnapShot = _size;

    // sizeSnapShot <= capacitySnapShot in recycle pool
    shortBuffer.Reserve(sizeSnapShot);

    uint8_t capacitySnapShot = 0;
    uint8_t* bufferSnapShot = _buffer;
    do {
        capacitySnapShot = _capacity;

        MEMORY_BARRIER();

        bufferSnapShot = _buffer;

        BufferMemoryCopy((uint8_t*)shortBuffer._buffer, shortBuffer._capacity, (uint8_t*)bufferSnapShot,
                         capacitySnapShot, _multiValue, sizeSnapShot);
    } while (!_isBufferValid || _buffer != bufferSnapShot || _capacity > capacitySnapShot);

    shortBuffer._size = sizeSnapShot;
    return true;
}

void ShortBuffer::Reserve(uint8_t capacity)
{
    if (_buffer != NULL) {
        if (_capacity >= capacity)
            return;
    }

    size_t docItemSize = capacity * _multiValue->GetTotalSize();

    uint8_t* newBuffer = (uint8_t*)Allocate(docItemSize);

    if (_buffer != NULL) {
        BufferMemoryCopy(newBuffer, capacity, (uint8_t*)_buffer, _capacity, _multiValue, _size);
        ReleaseBuffer((uint8_t*)_buffer, _capacity);
    }

    _buffer = newBuffer;
    _capacity = capacity;
}

uint8_t ShortBuffer::AllocatePlan(uint8_t curCapacity)
{
    if (curCapacity < 2) {
        return 2;
    } else if (curCapacity < 16) {
        return 16;
    }
    return MAX_DOC_PER_RECORD;
}
} // namespace indexlib::index
