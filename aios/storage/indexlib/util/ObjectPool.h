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

#include "autil/CommonMacros.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace util {

template <class T>
class ObjectPool
{
public:
    ObjectPool();
    ~ObjectPool();

public:
    void Init(int32_t elemCount);
    T* Alloc();
    void Free(T* object);
    uint32_t GetElemCount() const { return mElemCount; }

private:
    void DoInit();

private:
    T* mObjects;
    uint32_t* mFreeObjectIndexes;
    int32_t mFreeCursor;
    int32_t mElemCount;
    bool mInited;
};

template <class T>
inline ObjectPool<T>::ObjectPool()
    : mObjects(NULL)
    , mFreeObjectIndexes(NULL)
    , mFreeCursor(-1)
    , mElemCount(0)
    , mInited(false)
{
}

template <class T>
inline ObjectPool<T>::~ObjectPool()
{
    delete[] mObjects;
    delete[] mFreeObjectIndexes;
}

template <class T>
inline void ObjectPool<T>::Init(int32_t elemCount)
{
    mElemCount = elemCount;
}

template <class T>
inline void ObjectPool<T>::DoInit()
{
    if (mObjects) {
        delete[] mObjects;
    }
    if (mFreeObjectIndexes) {
        delete[] mFreeObjectIndexes;
    }

    mFreeCursor = mElemCount - 1;
    mObjects = new T[mElemCount];

    mFreeObjectIndexes = new uint32_t[mElemCount];
    for (int i = 0; i < mElemCount; ++i) {
        mObjects[i].SetOwner(this);
        mFreeObjectIndexes[i] = i;
    }
    mInited = true;
}

template <class T>
inline T* ObjectPool<T>::Alloc()
{
    if (unlikely(!mInited)) {
        DoInit();
    }
    assert(mFreeCursor >= 0);
    return mObjects + mFreeObjectIndexes[mFreeCursor--];
}

template <class T>
inline void ObjectPool<T>::Free(T* object)
{
    assert(mFreeCursor < mElemCount - 1);
    assert(object - mObjects >= 0 && object - mObjects < mElemCount);

    mFreeObjectIndexes[++mFreeCursor] = object - mObjects;
}
}} // namespace indexlib::util
