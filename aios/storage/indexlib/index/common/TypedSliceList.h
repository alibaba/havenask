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

#include "indexlib/base/Define.h"
#include "indexlib/index/common/RadixTree.h"

namespace indexlib::index {

class TypedSliceListBase
{
};

template <typename T>
class TypedSliceList : public TypedSliceListBase
{
public:
    typedef autil::mem_pool::Pool Pool;
    TypedSliceList(uint32_t slotNum, uint32_t sliceLength, Pool* pool) : _size(0)
    {
        void* buffer = pool->allocate(sizeof(index::RadixTree));
        _data = new (buffer) index::RadixTree(slotNum, sliceLength, pool, sizeof(T));
    }

    ~TypedSliceList() {}

public:
    void PushBack(const T& value);
    void Update(uint32_t index, const T& value);
    void Read(uint32_t index, T& value) const;
    void Read(uint32_t index, T*& buffer) const;

    T& operator[](uint32_t index)
    {
        assert(index < _size);
        T* buffer;
        Read(index, buffer);
        return *buffer;
    }

    uint64_t Size() const { return _size; }

    uint32_t GetSliceNum() const { return _data->GetSliceNum(); }
    uint8_t* GetSlice(uint32_t sliceId, uint32_t& sliceLength) const
    {
        sliceLength = GetSliceSize(sliceId);
        return _data->GetSlice(sliceId);
    }

private:
    uint32_t GetSliceSize(uint32_t sliceId) const;

private:
    index::RadixTree* _data;
    volatile uint64_t _size;

private:
    friend class TypedSliceListTest;
};

///////////////////////////////////////////////////////////
template <typename T>
inline void TypedSliceList<T>::Update(uint32_t index, const T& value)
{
    T* data = (T*)_data->Search(index);
    assert(data != NULL);
    *data = value;
}

template <typename T>
inline void TypedSliceList<T>::PushBack(const T& value)
{
    assert(_data);
    T* data = (T*)_data->OccupyOneItem();
    *data = value;
    MEMORY_BARRIER();
    _size = _size + 1;
}

template <typename T>
inline void TypedSliceList<T>::Read(uint32_t index, T*& buffer) const
{
    buffer = (T*)_data->Search(index);
}

template <typename T>
inline void TypedSliceList<T>::Read(uint32_t index, T& value) const
{
    T* buffer;
    Read(index, buffer);
    value = *buffer;
}

template <typename T>
inline uint32_t TypedSliceList<T>::GetSliceSize(uint32_t sliceId) const
{
    return _data->GetSliceSize(sliceId);
}
} // namespace indexlib::index
