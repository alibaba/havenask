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
#include <sstream>

#include "autil/mem_pool/PoolBase.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/VectorTyped.h"

namespace indexlib { namespace util {

// NOTE: use shared_ptr with array_deleter instead of shared_array(which is in TR2)
template <class T>
struct SharedArrayDeleter {
    void operator()(T* p) { delete[] p; }
};

template <class T>
class SliceArray
{
public:
    SliceArray(uint64_t sliceLen, uint32_t initSliceNum = 1, autil::mem_pool::PoolBase* pool = NULL,
               bool isOwner = false);
    SliceArray(const SliceArray& src);
    ~SliceArray();

public:
    uint64_t GetSliceLen() const noexcept { return _sliceLen; }
    uint32_t GetSliceNum() const noexcept { return _sliceVector.Size(); }
    int64_t GetMaxValidIndex() const noexcept { return _maxValidIndex; }
    int64_t GetDataSize() const noexcept { return GetMaxValidIndex() + 1; }
    uint64_t GetSliceDataLen(uint32_t sliceIndex) const noexcept
    {
        return sliceIndex >= _sliceVector.Size()
                   ? 0
                   : (sliceIndex < _sliceVector.Size() - 1 ? _sliceLen : GetLastSliceDataLen());
    }

    uint64_t GetLastSliceDataLen() const noexcept
    {
        return (_maxValidIndex == (int32_t)-1) ? 0 : _maxValidIndex % _sliceLen + 1;
    }

public:
    // Normal API
    inline const T& operator[](int64_t index) const noexcept;
    inline void Read(int64_t index, T* buf, uint32_t count) const;
    inline void Set(int64_t index, const T& t);
    inline void ReleaseSlice(uint32_t sliceIndex);
    inline const T* Get(int64_t index) const noexcept;

    // Internel API

    // TODO: GetSlice not allocate slice,
    // replace GetSliceWithoutAllocate with GetSlice
    T* GetSlice(uint32_t sliceIndex) noexcept(false);
    T* AllocateSlice(uint32_t sliceIndex) noexcept(false);
    T* GetSliceWithoutAllocate(uint32_t sliceIndex) noexcept;
    const T* GetSlice(uint32_t sliceIndex) const noexcept;
    const T* GetSliceAndZeroTail(uint32_t sliceIndex);
    void SetMaxValidIndex(int64_t index) { _maxValidIndex = index; }
    void ReplaceSlice(int32_t index, T* slice);

protected:
    void Reset();

protected:
    uint64_t _sliceLen;
    int64_t _maxValidIndex; // == max index you set

    util::VectorTyped<T*> _sliceVector;

    autil::mem_pool::PoolBase* _pool;
    bool _isOwner;

private:
    AUTIL_LOG_DECLARE();
};

typedef SliceArray<int32_t> Int32SliceArray;
typedef std::shared_ptr<Int32SliceArray> Int32SliceArrayPtr;

typedef SliceArray<uint32_t> UInt32SliceArray;
typedef std::shared_ptr<UInt32SliceArray> UInt32SliceArrayPtr;

////////////////////////////////////////////////////////////////////////

AUTIL_LOG_SETUP_TEMPLATE(indexlib.util, SliceArray, T);

template <class T>
SliceArray<T>::SliceArray(uint64_t sliceLen, uint32_t initSliceNum, autil::mem_pool::PoolBase* pool, bool isOwner)
    : _sliceLen(sliceLen)
    , _maxValidIndex(-1)
    , _pool(pool)
    , _isOwner(isOwner)
{
    assert(sliceLen > 0 && initSliceNum > 0);
    _sliceVector.Reserve(initSliceNum);
}

template <class T>
SliceArray<T>::SliceArray(const SliceArray& src)
    : _sliceLen(src._sliceLen)
    , _maxValidIndex(src._maxValidIndex)
    , _sliceVector(src._sliceVector)
    , _pool(src._pool)
    , _isOwner(src._isOwner)
{
}

template <class T>
SliceArray<T>::~SliceArray()
{
    Reset();
}

template <class T>
void SliceArray<T>::Reset()
{
    _maxValidIndex = -1;
    for (size_t i = 0; i < _sliceVector.Size(); ++i) {
        ReleaseSlice(i);
    }
    _sliceVector.Clear();
    if (_isOwner) {
        delete _pool;
    }
}

template <class T>
void SliceArray<T>::ReleaseSlice(uint32_t sliceIndex)
{
    if (sliceIndex >= _sliceVector.Size()) {
        INDEXLIB_THROW(util::InconsistentStateException, "Release slice fail, sliceIndex [%u], slice vector size [%lu]",
                       sliceIndex, _sliceVector.Size());
    }

    if (_sliceVector[sliceIndex] == NULL) {
        return;
    }
    if (_pool) {
        _pool->deallocate(_sliceVector[sliceIndex], _sliceLen * sizeof(T));
    } else {
        delete[] _sliceVector[sliceIndex];
    }
    _sliceVector[sliceIndex] = NULL;
}

template <class T>
T* SliceArray<T>::AllocateSlice(uint32_t sliceIndex) noexcept(false)
{
    if (sliceIndex < _sliceVector.Size() && _sliceVector[sliceIndex] != NULL) {
        INDEXLIB_THROW(util::InconsistentStateException, "Allocate slice fail, slice [%u] exist", sliceIndex);
    }
    if (sliceIndex > _sliceVector.Size()) {
        INDEXLIB_THROW(util::InconsistentStateException, "Allocate slice fail, slice [%u], sliceNum [%lu]", sliceIndex,
                       _sliceVector.Size());
    }

    if (sliceIndex == _sliceVector.Size()) {
        _sliceVector.Resize(sliceIndex + 1);
    }

    if (_pool != NULL) {
        void* pAddr = _pool->allocate(_sliceLen * sizeof(T));
        _sliceVector[sliceIndex] = new (pAddr) T[_sliceLen];
    } else {
        _sliceVector[sliceIndex] = new T[_sliceLen];
    }

    return _sliceVector[sliceIndex];
}

template <class T>
T* SliceArray<T>::GetSlice(uint32_t sliceIndex) noexcept(false)
{
    if (sliceIndex < _sliceVector.Size()) {
        return _sliceVector[sliceIndex];
    }

    uint32_t i = _sliceVector.Size();
    _sliceVector.Resize(sliceIndex + 1);
    for (; i < _sliceVector.Size(); ++i) {
        AllocateSlice(i);
    }

    return _sliceVector[sliceIndex];
}

template <class T>
inline const T* SliceArray<T>::GetSlice(uint32_t sliceIndex) const noexcept
{
    assert(sliceIndex < _sliceVector.Size());
    return _sliceVector[sliceIndex];
}

template <class T>
T* SliceArray<T>::GetSliceWithoutAllocate(uint32_t sliceIndex) noexcept
{
    assert(sliceIndex < _sliceVector.Size());
    return _sliceVector[sliceIndex];
}

template <class T>
const T* SliceArray<T>::GetSliceAndZeroTail(uint32_t sliceIndex)
{
    assert(sliceIndex < _sliceVector.Size());
    if (sliceIndex < (_sliceVector.Size() - 1)) {
        return _sliceVector[sliceIndex];
    } else {
        T* sliceData = _sliceVector[sliceIndex];
        uint32_t dataNum = (sliceIndex + 1) * _sliceLen;
        uint64_t validDataNum = (uint64_t)_maxValidIndex + 1;
        if (dataNum > validDataNum) {
            memset(sliceData + (validDataNum % _sliceLen), 0, (dataNum - validDataNum) * sizeof(T));
        }
        return sliceData;
    }
}

template <class T>
inline const T& SliceArray<T>::operator[](const int64_t index) const noexcept
{
    return *Get(index);
}

template <typename T>
inline const T* SliceArray<T>::Get(int64_t index) const noexcept
{
    assert(index <= _maxValidIndex);

    uint32_t sliceIndex = index / _sliceLen;
    uint32_t offset = index % _sliceLen;
    return &((_sliceVector[sliceIndex])[offset]);
}

template <class T>
inline void SliceArray<T>::Read(int64_t index, T* buf, uint32_t count) const
{
    uint32_t sliceIndex = index / _sliceLen;
    uint32_t offset = index % _sliceLen;
    uint32_t left = count;
    T* currentBuf = buf;

    while (true) {
        uint32_t currentLeft = _sliceLen - offset;
        uint32_t toCopy = left < currentLeft ? left : currentLeft;

        const T* slice = GetSlice(sliceIndex);
        memcpy((void*)currentBuf, (const void*)(slice + offset), toCopy * sizeof(T));
        currentBuf += toCopy;

        left -= toCopy;
        sliceIndex++;
        offset = 0;

        if (left == 0) {
            break;
        }
    }
}

template <class T>
inline void SliceArray<T>::Set(const int64_t index, const T& value)
{
    uint32_t sliceIndex = index / _sliceLen;
    uint32_t offset = index % _sliceLen;

    T* slice = GetSlice(sliceIndex);
    if (index > _maxValidIndex) {
        _maxValidIndex = index;
    }
    slice[offset] = value;
}

template <class T>
void SliceArray<T>::ReplaceSlice(int32_t sliceIndex, T* slice)
{
    if ((size_t)sliceIndex >= _sliceVector.Size()) {
        std::stringstream ss;
        ss << "Replace slice FAIL, slice number = " << _sliceVector.Size() << ", slice index = " << sliceIndex;
        INDEXLIB_THROW(util::InconsistentStateException, "%s", ss.str().c_str());
    }

    memcpy(_sliceVector[sliceIndex], slice, sizeof(T) * _sliceLen);

    if (slice != NULL && (int32_t)((sliceIndex + 1) * _sliceLen - 1) > _maxValidIndex) {
        _maxValidIndex = (sliceIndex + 1) * _sliceLen - 1;
    }
}
}} // namespace indexlib::util
