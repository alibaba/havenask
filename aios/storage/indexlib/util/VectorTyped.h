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

#include "autil/DataBuffer.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib { namespace util {

template <typename T>
class VectorTyped
{
public:
    class Iterator
    {
    public:
        Iterator(T* data = NULL, size_t cursor = 0) : _data(data), _cursor(cursor) {}
        bool operator!=(const Iterator& other) const { return _cursor != other._cursor; }

        bool operator==(const Iterator& other) const { return _cursor == other._cursor; }

        Iterator& operator++()
        {
            ++_cursor;
            return *this;
        }

        size_t operator-(const Iterator& other) { return _cursor - other._cursor; }

        T& operator*() { return _data[_cursor]; }

    private:
        T* _data;
        size_t _cursor;
    };

public:
    VectorTyped(autil::mem_pool::PoolBase* pool = NULL);
    ~VectorTyped();

    VectorTyped(const VectorTyped& src);

public:
    T& operator[](size_t index);
    const T& operator[](size_t index) const;
    void Resize(size_t newSize);
    void PushBack(const T& val);
    VectorTyped<T>& operator=(const VectorTyped& src);
    size_t Size() const { return _dataCursor; }
    void Clear();
    void Reserve(size_t newSize);
    size_t Capacity() const { return _allocSize; }

    Iterator Begin() const { return Iterator(_data, 0); }
    Iterator End() const { return Iterator(_data, Size()); }

    void Serialize(autil::DataBuffer& dataBuffer) const;
    void Deserialize(autil::DataBuffer& dataBuffer);

private:
    size_t _dataCursor;
    size_t _allocSize;
    T* _data;
    autil::mem_pool::PoolBase* _pool;
    mutable autil::ReadWriteLock _lock;
    static const size_t MIN_SIZE = 4;

    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////

AUTIL_LOG_SETUP_TEMPLATE(indexlib.util, VectorTyped, T);

template <typename T>
inline VectorTyped<T>::VectorTyped(autil::mem_pool::PoolBase* pool)
    : _dataCursor(0)
    , _allocSize(0)
    , _data(NULL)
    , _pool(NULL)
    , _lock(autil::ReadWriteLock::PREFER_WRITER)
{
}

template <typename T>
VectorTyped<T>::VectorTyped(const VectorTyped& src)
    : _dataCursor(0)
    , _allocSize(0)
    , _data(NULL)
    , _pool(src._pool)
    , _lock(autil::ReadWriteLock::PREFER_WRITER)
{
    autil::ScopedReadWriteLock lock(src._lock, 'r');
    _dataCursor = src._dataCursor;
    _allocSize = src._allocSize;
    _data = IE_POOL_NEW_VECTOR2<T>(_pool, _allocSize);

    memcpy((void*)_data, src._data, sizeof(T) * _allocSize);
}

template <typename T>
inline VectorTyped<T>::~VectorTyped()
{
    if (_data) {
        IE_POOL_DELETE_VECTOR2(_pool, _data, _allocSize);
        _data = NULL;
    }
}

template <typename T>
inline T& VectorTyped<T>::operator[](size_t index)
{
    assert(index < _dataCursor);
    return _data[index];
}

template <typename T>
inline const T& VectorTyped<T>::operator[](size_t index) const
{
    assert(index < _dataCursor);
    return _data[index];
}

template <typename T>
void VectorTyped<T>::Reserve(size_t newSize)
{
    if (newSize <= _allocSize) {
        return;
    }

    if (newSize < MIN_SIZE) {
        newSize = MIN_SIZE;
    }

    AUTIL_LOG(DEBUG, "Allocate %d items", (int32_t)newSize);
    T* data = IE_POOL_NEW_VECTOR2<T>(_pool, newSize);
    memset((void*)data, 0, newSize * sizeof(T));

    memcpy((void*)data, _data, _allocSize * sizeof(T));

    T* oldData = _data;
    size_t oldAllocSize = _allocSize;

    autil::ScopedReadWriteLock lock(_lock, 'w');
    _data = data;
    _allocSize = newSize;
    if (oldData) {
        IE_POOL_DELETE_VECTOR2(_pool, oldData, oldAllocSize);
    }
}

template <typename T>
void VectorTyped<T>::PushBack(const T& val)
{
    if (_dataCursor == _allocSize) {
        size_t newSize = _allocSize * 2;
        if (newSize < MIN_SIZE) {
            newSize = MIN_SIZE;
        }
        Reserve(newSize);
    }

    _data[_dataCursor++] = val;
}

template <typename T>
VectorTyped<T>& VectorTyped<T>::operator=(const VectorTyped& src)
{
    T* data;
    uint32_t allocSize;
    uint32_t dataCursor;
    {
        autil::ScopedReadWriteLock lock(src._lock, 'r');
        dataCursor = src._dataCursor;
        allocSize = src._allocSize;
        data = IE_POOL_NEW_VECTOR2<T>(_pool, allocSize);
        for (size_t i = 0; i < allocSize; ++i) {
            data[i] = src._data[i];
        }
    }
    T* oldData = _data;
    size_t oldAllocSize = _allocSize;

    autil::ScopedReadWriteLock lock(_lock, 'w');
    _data = data;
    _allocSize = allocSize;
    _dataCursor = dataCursor;
    if (oldData) {
        IE_POOL_DELETE_VECTOR2(_pool, oldData, oldAllocSize);
    }

    return *this;
}

template <typename T>
void VectorTyped<T>::Resize(size_t newSize)
{
    size_t newAllocSize = _allocSize;
    if (newAllocSize < MIN_SIZE) {
        newAllocSize = MIN_SIZE;
    }
    while (newAllocSize < newSize) {
        newAllocSize *= 2;
    }

    if (newAllocSize == _allocSize) {
        _dataCursor = newSize;
        return;
    }

    AUTIL_LOG(DEBUG, "Resize to %d", (int32_t)newAllocSize);

    Reserve(newAllocSize);
    _dataCursor = newSize;
}

template <typename T>
void VectorTyped<T>::Clear()
{
    autil::ScopedReadWriteLock lock(_lock, 'w');
    if (_data) {
        IE_POOL_DELETE_VECTOR2(_pool, _data, _allocSize);
        _data = NULL;
    }
    _allocSize = 0;
    _dataCursor = 0;
}

template <typename T>
void VectorTyped<T>::Serialize(autil::DataBuffer& dataBuffer) const
{
    uint32_t size = Size();
    dataBuffer.write(size);
    for (size_t i = 0; i < size; ++i) {
        dataBuffer.write(_data[i]);
    }
}

template <typename T>
void VectorTyped<T>::Deserialize(autil::DataBuffer& dataBuffer)
{
    uint32_t size = 0;
    dataBuffer.read(size);
    Resize(size);
    for (uint32_t i = 0; i < size; ++i) {
        dataBuffer.read(_data[i]);
    }
}
}} // namespace indexlib::util
