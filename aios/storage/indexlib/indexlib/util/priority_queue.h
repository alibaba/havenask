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
#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace util {

template <typename T, typename Comparator>
class PriorityQueue
{
public:
    PriorityQueue();
    PriorityQueue(const Comparator& cmp);
    ~PriorityQueue();

public:
    void Init();
    void SetComparator(const Comparator& cmp);
    const Comparator& GetComparator() const;
    void Push(const T& element);
    void PushBack(const T& element);
    T Pop();
    T PopAndPush(const T& element);
    const T& Top() const;
    void Clear();
    size_t Size() const;
    size_t Capacity() const;
    const T& operator[](size_t index) const;
    T* GetItems() { return mHeap + 1; }

public:
    // for test
    void SetCapacity(size_t capacity);

protected:
    inline void UpHeap()
    {
        size_t i = mSize;
        T node = mHeap[i];
        size_t j = i >> 1;

        while (j > 0 && !mComparator(node, mHeap[j])) {
            mHeap[i] = mHeap[j];
            i = j;
            j = j >> 1;
        }
        mHeap[i] = node;
    }

    inline void DownHeap()
    {
        size_t i = 1;
        T node = mHeap[i];
        size_t j = i << 1;
        size_t k = j + 1;

        if (k <= mSize && !mComparator(mHeap[k], mHeap[j])) {
            j = k;
        }

        while (j <= mSize && !mComparator(mHeap[j], node)) {
            mHeap[i] = mHeap[j];
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= mSize && !mComparator(mHeap[k], mHeap[j])) {
                j = k;
            }
        }
        mHeap[i] = node;
    }

private:
    void Reallocate();

protected:
    T* mHeap;
    size_t mCapacity;
    size_t mSize;
    Comparator mComparator;
    static const T STATIC_T;

private:
    static const size_t DEFAULT_CAPACITY = 2;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE2(util, PriorityQueue);

//////////////////////////////////////////////////
template <typename T, typename Comparator>
const T PriorityQueue<T, Comparator>::STATIC_T = T();

template <typename T, typename Comparator>
PriorityQueue<T, Comparator>::PriorityQueue() : mHeap(NULL)
                                              , mCapacity(DEFAULT_CAPACITY)
                                              , mSize(0)
{
}

template <typename T, typename Comparator>
PriorityQueue<T, Comparator>::PriorityQueue(const Comparator& cmp)
    : mHeap(NULL)
    , mCapacity(DEFAULT_CAPACITY)
    , mSize(0)
    , mComparator(cmp)
{
}

template <typename T, typename Comparator>
PriorityQueue<T, Comparator>::~PriorityQueue()
{
    if (mHeap) {
        delete[] mHeap;
    }
}

template <typename T, typename Comparator>
void PriorityQueue<T, Comparator>::Init()
{
    assert(mHeap == NULL);
    mHeap = new T[mCapacity];
}

template <typename T, typename Comparator>
void PriorityQueue<T, Comparator>::SetComparator(const Comparator& cmp)
{
    mComparator = cmp;
}

template <typename T, typename Comparator>
const Comparator& PriorityQueue<T, Comparator>::GetComparator() const
{
    return mComparator;
}

template <typename T, typename Comparator>
void PriorityQueue<T, Comparator>::Reallocate()
{
    size_t newCapacity = mCapacity << 1;
    T* newHeap = new T[newCapacity];
    assert(newHeap);
    memcpy(newHeap, mHeap, sizeof(T) * (mSize + 1));
    delete[] mHeap;
    mHeap = newHeap;
    mCapacity = newCapacity;
}

template <typename T, typename Comparator>
void PriorityQueue<T, Comparator>::Push(const T& element)
{
    // mHeap[0] is not used for this heap implementation
    if (mSize + 1 >= mCapacity) {
        Reallocate();
    }
    mSize++;
    mHeap[mSize] = element;
    UpHeap();
}

template <typename T, typename Comparator>
void PriorityQueue<T, Comparator>::PushBack(const T& element)
{
    if (mSize + 1 >= mCapacity) {
        Reallocate();
    }
    mSize++;
    mHeap[mSize] = element;
}

template <typename T, typename Comparator>
T PriorityQueue<T, Comparator>::Pop()
{
    if (mSize == 1) {
        mSize = 0;
        return mHeap[1];
    } else if (mSize > 0) {
        T result = mHeap[1];
        mHeap[1] = mHeap[mSize];
        mHeap[mSize] = T();
        mSize--;
        DownHeap();
        return result;
    }
    return T();
}

template <typename T, typename Comparator>
T PriorityQueue<T, Comparator>::PopAndPush(const T& element)
{
    if (mSize > 0) {
        T result = mHeap[1];
        mHeap[1] = element;
        DownHeap();
        return result;
    } else {
        mHeap[1] = element;
        return T();
    }
}

template <typename T, typename Comparator>
const T& PriorityQueue<T, Comparator>::Top() const
{
    if (mSize > 0) {
        return mHeap[1];
    }
    return STATIC_T;
}

template <typename T, typename Comparator>
void PriorityQueue<T, Comparator>::Clear()
{
    for (size_t i = 0; i <= mSize; i++) {
        mHeap[i] = T();
    }
    mSize = 0;
}

template <typename T, typename Comparator>
size_t PriorityQueue<T, Comparator>::Size() const
{
    return mSize;
}

template <typename T, typename Comparator>
size_t PriorityQueue<T, Comparator>::Capacity() const
{
    return mCapacity;
}

template <typename T, typename Comparator>
void PriorityQueue<T, Comparator>::SetCapacity(size_t capacity)
{
    mCapacity = capacity;
}

template <typename T, typename Comparator>
const T& PriorityQueue<T, Comparator>::operator[](size_t index) const
{
    assert(index < mSize);
    return mHeap[index + 1];
}
}} // namespace indexlib::util
