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

#include "indexlib/util/slice_array/SliceArray.h"

namespace indexlib { namespace util {

template <class T>
class AlignedSliceArray : public SliceArray<T>
{
public:
    AlignedSliceArray(uint64_t sliceLen, uint32_t initSliceNum = 1, autil::mem_pool::PoolBase* pool = NULL,
                      bool isOwner = false) noexcept;

    AlignedSliceArray(const AlignedSliceArray<T>& src) noexcept;

    ~AlignedSliceArray() noexcept;

public:
    inline const T& operator[](int64_t offset) const noexcept;
    inline void Set(int64_t offset, const T& value) noexcept(false);
    inline void SetList(int64_t offset, const T* value, int64_t size) noexcept(false);
    inline void Read(int64_t offset, T* buf, uint32_t count) const noexcept(false);
    AlignedSliceArray* Clone() const noexcept;

    int64_t OffsetToSliceIdx(int64_t offset) const noexcept { return offset >> _powerOf2; }
    int64_t SliceIdxToOffset(int64_t sliceIdx) const noexcept { return sliceIdx << _powerOf2; }

    T* OffsetToAddress(int64_t offset) noexcept
    {
        size_t sliceIdx = offset >> _powerOf2;
        size_t cursor = offset & _powerMark;
        T* sliceAddress = this->GetSliceWithoutAllocate(sliceIdx);
        if (unlikely(sliceAddress == NULL)) {
            return NULL;
        }
        return sliceAddress + cursor;
    }

protected:
    uint32_t _powerOf2;
    uint32_t _powerMark;

private:
    friend class AlignedSliceArrayTest;
};

typedef AlignedSliceArray<int32_t> Int32AlignedSliceArray;
typedef std::shared_ptr<Int32AlignedSliceArray> Int32AlignedSliceArrayPtr;

typedef AlignedSliceArray<uint32_t> UInt32AlignedSliceArray;
typedef std::shared_ptr<UInt32AlignedSliceArray> UInt32AlignedSliceArrayPtr;

////////////////////////////////////////////////////////////////////////

template <class T>
AlignedSliceArray<T>::AlignedSliceArray(uint64_t sliceLen, uint32_t initSliceNum, autil::mem_pool::PoolBase* pool,
                                        bool isOwner) noexcept
    : SliceArray<T>(sliceLen, initSliceNum, pool, isOwner)
{
    assert((int64_t)sliceLen > 0 && initSliceNum > 0); // sliceLen must >=0 && <= 2^31
    // align _sliceLen
    this->_sliceLen = 1;
    _powerOf2 = 0;
    while (this->_sliceLen < sliceLen) {
        this->_sliceLen <<= 1;
        _powerOf2++;
    }
    _powerMark = this->_sliceLen - 1;
}

template <class T>
AlignedSliceArray<T>::AlignedSliceArray(const AlignedSliceArray<T>& src) noexcept
    : SliceArray<T>(src)
    , _powerOf2(src._powerOf2)
    , _powerMark(src._powerMark)
{
}

template <class T>
AlignedSliceArray<T>::~AlignedSliceArray() noexcept
{
}

template <class T>
inline const T& AlignedSliceArray<T>::operator[](int64_t offset) const noexcept
{
    assert(offset <= this->_maxValidIndex);

    int64_t sliceIndex = offset >> _powerOf2;
    uint32_t sliceCursor = offset & _powerMark;

    return (this->_sliceVector[sliceIndex])[sliceCursor];
}

template <class T>
inline void AlignedSliceArray<T>::Set(int64_t offset, const T& value) noexcept(false)
{
    int64_t sliceIndex = offset >> _powerOf2;
    uint32_t sliceCursor = offset & _powerMark;

    T* slice = this->GetSlice(sliceIndex);
    slice[sliceCursor] = value;
    if (offset > this->_maxValidIndex) {
        this->_maxValidIndex = offset;
    }
}

template <class T>
inline void AlignedSliceArray<T>::SetList(int64_t offset, const T* value, int64_t size) noexcept(false)
{
    int64_t sliceIndex = offset >> _powerOf2;
    uint32_t sliceCursor = offset & _powerMark;

    uint64_t sliceRemain = this->_sliceLen - sliceCursor;
    uint64_t toSetNum = 0;
    int64_t remainSize = size;

    while (remainSize > 0) {
        value += toSetNum;
        T* slice = this->GetSlice(sliceIndex++);
        if (remainSize <= (int64_t)sliceRemain) {
            toSetNum = remainSize;
        } else {
            toSetNum = sliceRemain;
            sliceRemain = this->_sliceLen;
        }
        memcpy(slice + sliceCursor, value, sizeof(T) * toSetNum);
        remainSize -= toSetNum;
        sliceCursor = 0;
    }

    if ((offset + size) > this->_maxValidIndex) {
        this->_maxValidIndex = offset + size - 1;
    }
}

template <class T>
inline void AlignedSliceArray<T>::Read(int64_t offset, T* buf, uint32_t count) const noexcept(false)
{
    int64_t sliceIndex = offset >> _powerOf2;
    uint32_t sliceCursor = offset & _powerMark;

    uint32_t left = count;
    T* currentBuf = buf;

    while (true) {
        uint32_t currentLeft = this->_sliceLen - sliceCursor;
        uint32_t toCopy = left < currentLeft ? left : currentLeft;

        const T* slice = this->GetSlice(sliceIndex++);
        memcpy((void*)currentBuf, (const void*)(slice + sliceCursor), toCopy * sizeof(T));
        currentBuf += toCopy;

        left -= toCopy;
        sliceCursor = 0;

        if (left == 0) {
            break;
        }
    }
}

template <class T>
AlignedSliceArray<T>* AlignedSliceArray<T>::Clone() const noexcept
{
    return new AlignedSliceArray<T>(*this);
}
}} // namespace indexlib::util
