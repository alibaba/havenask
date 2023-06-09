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
#include <cassert>
#include <memory>

#include "autil/Log.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/MMapAllocator.h"

namespace indexlib { namespace util {

template <typename T>
class MMapVector
{
public:
    MMapVector() : _size(0), _capacity(0), _data(NULL) {}

    ~MMapVector() { Release(); }

public:
    void Init(const MMapAllocatorPtr& allocator, size_t capacity)
    {
        _capacity = capacity;
        _allocator = allocator;
        _data = (T*)_allocator->allocate(capacity * sizeof(T));
    }

    bool Empty() const { return _size == 0; }
    bool IsFull() const { return _size >= _capacity; }

    const T& operator[](size_t pos) const;
    T& operator[](size_t pos);

    void PushBack(const T& value);
    size_t Size() const;
    bool operator==(const MMapVector<T>& other) const;
    bool operator!=(const MMapVector<T>& other) const { return !(*this == other); }

    size_t GetUsedBytes() const;
    size_t Capacity() const { return _capacity; }

private:
    void Release()
    {
        if (_data) {
            assert(_allocator);
            _allocator->deallocate(_data, _capacity * sizeof(T));
            _data = NULL;
            _allocator.reset();
        }
        _size = 0;
        _capacity = 0;
    }

private:
    MMapAllocatorPtr _allocator;
    size_t volatile _size;
    size_t _capacity;
    T* _data;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.util, MMapVector, T);

//////////////////////////////////////////////////
template <typename T>
inline const T& MMapVector<T>::operator[](size_t pos) const
{
    assert(_data);
    assert(pos < _size);
    return _data[pos];
}

template <typename T>
inline T& MMapVector<T>::operator[](size_t pos)
{
    assert(_data);
    assert(pos < _size);
    return _data[pos];
}

template <typename T>
inline void MMapVector<T>::PushBack(const T& value)
{
    assert(_data);
    if (IsFull()) {
        INDEXLIB_FATAL_ERROR(UnSupported, "vector is full [size:%lu], not support extend size", _size);
    }
    _data[_size] = value;
    _size = _size + 1;
}

template <typename T>
inline size_t MMapVector<T>::Size() const
{
    return _size;
}

template <typename T>
inline bool MMapVector<T>::operator==(const MMapVector<T>& other) const
{
    if (_size != other._size) {
        return false;
    }

    for (size_t i = 0; i < _size; i++) {
        if (_data[i] != other._data[i]) {
            return false;
        }
    }
    return true;
}

template <typename T>
inline size_t MMapVector<T>::GetUsedBytes() const
{
    return _size * sizeof(T);
}
}} // namespace indexlib::util
