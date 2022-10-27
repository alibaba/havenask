#ifndef __INDEXLIB_MMAP_VECTOR_H
#define __INDEXLIB_MMAP_VECTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/misc/exception.h"
#include "indexlib/common_define.h"
#include "indexlib/util/mmap_allocator.h"

IE_NAMESPACE_BEGIN(util);

template <typename T>
class MMapVector
{
public:
    MMapVector()
        : mSize(0)
        , mCapacity(0)
        , mData(NULL)
    {}

    ~MMapVector()
    {
        Release();
    }

public:
    void Init(const MMapAllocatorPtr& allocator, size_t capacity)
    {
        mCapacity = capacity;
        mAllocator = allocator;
        mData = (T*)mAllocator->allocate(capacity * sizeof(T));
    }

    bool Empty() const { return mSize == 0; }
    bool IsFull() const { return mSize >= mCapacity; }
    
    const T& operator[](size_t pos) const;
    T& operator[](size_t pos);

    void PushBack(const T& value);
    size_t Size() const;
    bool operator==(const MMapVector<T>& other) const;
    bool operator!=(const MMapVector<T>& other) const
    { return !(*this == other); }

    size_t GetUsedBytes() const;
    size_t Capacity() const { return mCapacity; }
private:
    void Release()
    {
        if (mData)
        {
            assert(mAllocator);
            mAllocator->deallocate(mData, mCapacity * sizeof(T));
            mData = NULL;
            mAllocator.reset();
        }
        mSize = 0;
        mCapacity = 0;
    }

private:
    MMapAllocatorPtr mAllocator;
    size_t volatile mSize;
    size_t mCapacity;
    T* mData;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(util, MMapVector);

//////////////////////////////////////////////////
template <typename T>
inline const T& MMapVector<T>::operator[](size_t pos) const
{
    assert(mData);
    assert(pos < mSize);
    return mData[pos];
}

template <typename T>
inline T& MMapVector<T>::operator[](size_t pos)
{
    assert(mData);
    assert(pos < mSize);
    return mData[pos];
}

template <typename T>
inline void MMapVector<T>::PushBack(const T& value)
{
    assert(mData);
    if (IsFull())
    {
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "vector is full [size:%lu], not support extend size", mSize);
    }
    mData[mSize++] = value;
}

template <typename T>
inline size_t MMapVector<T>::Size() const
{
    return mSize;
}

template <typename T>
inline bool MMapVector<T>::operator==(const MMapVector<T>& other) const
{
    if (mSize != other.mSize)
    {
        return false;
    }

    for (size_t i = 0; i < mSize; i++)
    {
        if (mData[i] != other.mData[i])
        {
            return false;
        }
    }
    return true;
}

template <typename T>
inline size_t MMapVector<T>::GetUsedBytes() const
{
    return mSize * sizeof(T);
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_MMAP_VECTOR_H
