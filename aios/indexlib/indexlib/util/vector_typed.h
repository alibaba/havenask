#ifndef __INDEXLIB_VECTOR_TYPED_H
#define __INDEXLIB_VECTOR_TYPED_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <tr1/memory>
#include <autil/Lock.h>
#include <autil/DataBuffer.h>

#include "indexlib/util/profiling.h"


IE_NAMESPACE_BEGIN(util);

template <typename T>
class VectorTyped
{
public:
    class Iterator
    {
    public:
        Iterator(T* data = NULL, size_t cursor = 0)
            : mData(data)
            , mCursor(cursor)
        {}
        bool operator !=(const Iterator& other) const
        { return mCursor != other.mCursor; }

        bool operator ==(const Iterator& other) const
        { return mCursor == other.mCursor; }

        Iterator& operator ++()
        { ++mCursor; return *this; }

        size_t operator -(const Iterator& other)
        { return mCursor - other.mCursor; }

        T& operator *()
        { return mData[mCursor]; }

    private:
        T* mData;
        size_t mCursor;
    };

public:
    VectorTyped(autil::mem_pool::PoolBase* pool = NULL);
    ~VectorTyped();

    VectorTyped(const VectorTyped& src);
public:
    T& operator [] (size_t index);
    const T& operator [] (size_t index) const;
    void Resize(size_t newSize);
    void PushBack(const T& val);
    VectorTyped<T>& operator = (const VectorTyped& src);
    size_t Size() const { return mDataCursor; }
    void Clear();
    void Reserve(size_t newSize);
    size_t Capacity() const { return mAllocSize; }

    Iterator Begin() const { return Iterator(mData, 0); }
    Iterator End() const { return Iterator(mData, Size()); }

    void Serialize(autil::DataBuffer &dataBuffer) const;
    void Deserialize(autil::DataBuffer &dataBuffer);

private:
    size_t mDataCursor;
    size_t mAllocSize;
    T* mData;
    autil::mem_pool::PoolBase* mPool;
    mutable autil::ReadWriteLock mLock;
    static const size_t MIN_SIZE = 4;

    IE_LOG_DECLARE();
};

//////////////////////////////////////////////////

IE_LOG_SETUP_TEMPLATE(util, VectorTyped);

template <typename T>
inline VectorTyped<T>::VectorTyped(autil::mem_pool::PoolBase* pool)
    : mDataCursor(0)
    , mAllocSize(0)
    , mData(NULL)
    , mPool(NULL)
    , mLock(autil::ReadWriteLock::PREFER_WRITER)
{
}

template <typename T>
VectorTyped<T>::VectorTyped(const VectorTyped& src)
    : mDataCursor(0)
    , mAllocSize(0)
    , mData(NULL)
    , mPool(src.mPool)
    , mLock(autil::ReadWriteLock::PREFER_WRITER)
{
    autil::ScopedReadWriteLock lock(src.mLock, 'r');
    mDataCursor = src.mDataCursor;
    mAllocSize = src.mAllocSize;
    mData = IE_POOL_NEW_VECTOR2<T>(mPool, mAllocSize);
    PROCESS_PROFILE_COUNT(memory, VectorTyped, mAllocSize * sizeof(T));

    memcpy((void*)mData, src.mData, sizeof(T) * mAllocSize);
}

template <typename T>
inline VectorTyped<T>::~VectorTyped() 
{
    if (mData)
    {
        IE_POOL_DELETE_VECTOR2(mPool, mData, mAllocSize);
        mData = NULL;
        PROCESS_PROFILE_COUNT(memory, VectorTyped, -mAllocSize * sizeof(T));
    }
}

template <typename T>
inline T& VectorTyped<T>::operator [] (size_t index)
{
    assert(index < mDataCursor);
    return mData[index];
}

template <typename T>
inline const T& VectorTyped<T>::operator [] (size_t index) const
{
    assert(index < mDataCursor);
    return mData[index];
}

template <typename T>
void VectorTyped<T>::Reserve(size_t newSize)
{
    if (newSize <= mAllocSize)
    {
        return;
    }

    if (newSize < MIN_SIZE)
    {
        newSize = MIN_SIZE;
    }

    IE_LOG(DEBUG, "Allocate %d items", (int32_t)newSize);
    T* data = IE_POOL_NEW_VECTOR2<T>(mPool, newSize);
    PROCESS_PROFILE_COUNT(memory, VectorTyped, newSize * sizeof(T));
    memset((void*)data, 0, newSize * sizeof(T));

    memcpy((void*)data, mData, mAllocSize * sizeof(T));

    T* oldData = mData;
    size_t oldAllocSize = mAllocSize;

    autil::ScopedReadWriteLock lock(mLock, 'w');
    mData = data;
    mAllocSize = newSize;
    if (oldData)
    {
        PROCESS_PROFILE_COUNT(memory, VectorTyped, -sizeof(oldData));
        IE_POOL_DELETE_VECTOR2(mPool, oldData, oldAllocSize);
    }
}

template <typename T> 
void VectorTyped<T>::PushBack(const T& val)
{
    if (mDataCursor == mAllocSize)
    {
        size_t newSize = mAllocSize * 2;
        if (newSize < MIN_SIZE)
        {
            newSize = MIN_SIZE;
        }
        Reserve(newSize);
    }
    
    mData[mDataCursor++] = val;
}

template <typename T>
VectorTyped<T>& VectorTyped<T>::operator = (const VectorTyped& src)
{
    T* data;
    uint32_t allocSize;
    uint32_t dataCursor;
    {
        autil::ScopedReadWriteLock lock(src.mLock, 'r');
        dataCursor = src.mDataCursor;
        allocSize = src.mAllocSize;
        data = IE_POOL_NEW_VECTOR2<T>(mPool, allocSize);
        PROCESS_PROFILE_COUNT(memory, VectorTyped, allocSize * sizeof(T));
        for (size_t i = 0; i < allocSize; ++i)
        {
            data[i] = src.mData[i];
        }
    }
    T* oldData = mData;
    size_t oldAllocSize = mAllocSize;

    autil::ScopedReadWriteLock lock(mLock, 'w');
    mData = data;
    mAllocSize = allocSize;
    mDataCursor = dataCursor;
    if (oldData)
    {
        PROCESS_PROFILE_COUNT(memory, VectorTyped, -sizeof(oldData));
        IE_POOL_DELETE_VECTOR2(mPool, oldData, oldAllocSize);
    }

    return *this;
}

template <typename T>
void VectorTyped<T>::Resize(size_t newSize)
{
    size_t newAllocSize = mAllocSize;
    if (newAllocSize < MIN_SIZE)
    {
        newAllocSize = MIN_SIZE;
    }
    while (newAllocSize < newSize)
    {
        newAllocSize *= 2;
    }

    if (newAllocSize == mAllocSize)
    {
        mDataCursor = newSize;
        return;
    }

    IE_LOG(DEBUG, "Resize to %d", (int32_t)newAllocSize);

    Reserve(newAllocSize);
    mDataCursor = newSize;
}

template <typename T>
void VectorTyped<T>::Clear()
{
    autil::ScopedReadWriteLock lock(mLock, 'w');
    if (mData)
    {
        IE_POOL_DELETE_VECTOR2(mPool, mData, mAllocSize);
        mData = NULL;

        PROCESS_PROFILE_COUNT(memory, VectorTyped, mAllocSize * sizeof(T));
    }
    mAllocSize = 0;
    mDataCursor = 0;
}

template <typename T>
void VectorTyped<T>::Serialize(autil::DataBuffer &dataBuffer) const
{
    uint32_t size = Size();
    dataBuffer.write(size);
    for (size_t i = 0; i < size; ++i)
    {
        dataBuffer.write(mData[i]);
    }
}

template <typename T>
void VectorTyped<T>::Deserialize(autil::DataBuffer &dataBuffer)
{
    uint32_t size = 0;
    dataBuffer.read(size);
    Resize(size);
    for (uint32_t i = 0; i < size; ++i)
    {
        dataBuffer.read(mData[i]);
    }
}


IE_NAMESPACE_END(util);

#endif //__INDEXLIB_VECTOR_TYPED_H
