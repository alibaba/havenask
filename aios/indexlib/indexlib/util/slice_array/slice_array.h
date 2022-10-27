#ifndef __INDEXLIB_SLICE_ARRAY_H
#define __INDEXLIB_SLICE_ARRAY_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/metrics.h"
#include "indexlib/util/vector_typed.h"
#include <autil/mem_pool/PoolBase.h>
#include <sstream>

IE_NAMESPACE_BEGIN(util);

// NOTE: use shared_ptr with array_deleter instead of shared_array(which is in TR2)
template<class T>
struct SharedArrayDeleter
{
    void operator()(T* p) 
    { 
        delete [] p;
    }
};

template<class T>
class SliceArray
{
public:
    SliceArray(uint64_t sliceLen, uint32_t initSliceNum = 1,
               util::Metrics* metrics = NULL, 
               autil::mem_pool::PoolBase *pool = NULL, bool isOwner = false);
    SliceArray(const SliceArray& src);
    ~SliceArray();

public:
    uint64_t GetSliceLen() const { return mSliceLen; }
    uint32_t GetSliceNum() const { return mSliceVector.Size(); }
    int64_t GetMaxValidIndex() const { return mMaxValidIndex; }
    int64_t GetDataSize() const { return GetMaxValidIndex() + 1; }
    uint64_t GetSliceDataLen(uint32_t sliceIndex) const
    {
        return sliceIndex >= mSliceVector.Size() ? 0 :
            (sliceIndex < mSliceVector.Size() - 1 ?
             mSliceLen : GetLastSliceDataLen());
    }

    uint64_t GetLastSliceDataLen() const 
    { 
        return (mMaxValidIndex == (int32_t)-1) ?
            0 : mMaxValidIndex % mSliceLen + 1;
    }

public:
    // Normal API
    inline const T& operator[] (int64_t index) const;
    inline void Read(int64_t index, T* buf, uint32_t count) const;
    inline void Set(int64_t index, const T& t);
    inline void ReleaseSlice(uint32_t sliceIndex);

    // Internel API    
    
    //TODO: GetSlice not allocate slice,
    // replace GetSliceWithoutAllocate with GetSlice
    T* GetSlice(uint32_t sliceIndex);
    T* AllocateSlice(uint32_t sliceIndex);
    T* GetSliceWithoutAllocate(uint32_t sliceIndex);
    const T* GetSlice(uint32_t sliceIndex) const;
    const T* GetSliceAndZeroTail(uint32_t sliceIndex);
    void SetMaxValidIndex(int64_t index) { mMaxValidIndex = index; }
    void ReplaceSlice(int32_t index, T* slice);

protected:
    void Reset();

protected:
    uint64_t mSliceLen;
    int64_t mMaxValidIndex;  // == max index you set   

    util::VectorTyped<T*> mSliceVector;
    util::Metrics* mMetrics;

    autil::mem_pool::PoolBase *mPool;
    bool mIsOwner;

private:
    IE_LOG_DECLARE();
};

typedef SliceArray<int32_t> Int32SliceArray;
typedef std::tr1::shared_ptr<Int32SliceArray> Int32SliceArrayPtr;

typedef SliceArray<uint32_t> UInt32SliceArray;
typedef std::tr1::shared_ptr<UInt32SliceArray> UInt32SliceArrayPtr;

////////////////////////////////////////////////////////////////////////

IE_LOG_SETUP_TEMPLATE(common, SliceArray);

template<class T>
SliceArray<T>::SliceArray(uint64_t sliceLen, uint32_t initSliceNum,
                          util::Metrics* metrics,
                          autil::mem_pool::PoolBase *pool, bool isOwner)
    : mSliceLen(sliceLen)
    , mMaxValidIndex(-1)
    , mMetrics(metrics)
    , mPool(pool)
    , mIsOwner(isOwner)
{
    assert(sliceLen > 0 && initSliceNum > 0);
    mSliceVector.Reserve(initSliceNum);
}

template<class T>
SliceArray<T>::SliceArray(const SliceArray& src)
    : mSliceLen(src.mSliceLen)
    , mMaxValidIndex(src.mMaxValidIndex)
    , mSliceVector(src.mSliceVector)
    , mMetrics(src.mMetrics)
    , mPool(src.mPool)
    , mIsOwner(src.mIsOwner)
{
}

template<class T>
SliceArray<T>::~SliceArray() 
{
    Reset();
}

template<class T>
void SliceArray<T>::Reset()
{
    mMaxValidIndex = -1;
    for (size_t i = 0; i < mSliceVector.Size(); ++i)
    {
        ReleaseSlice(i);
    }
    mSliceVector.Clear();
    if (mIsOwner)
    {
        delete mPool;
    }
}

template<class T>
void SliceArray<T>::ReleaseSlice(uint32_t sliceIndex)
{
    if (sliceIndex >= mSliceVector.Size())
    {
        INDEXLIB_THROW(misc::InconsistentStateException,
                       "Release slice fail, sliceIndex [%u], slice vector size [%lu]",
                       sliceIndex, mSliceVector.Size());
    }

    if(mSliceVector[sliceIndex] == NULL)
    {
        return;
    }
    if (mPool)
    {
        mPool->deallocate(mSliceVector[sliceIndex], mSliceLen * sizeof(T));
    }
    else
    {
        delete[] mSliceVector[sliceIndex];
    }
    mSliceVector[sliceIndex] = NULL;
}

template<class T>
T* SliceArray<T>::AllocateSlice(uint32_t sliceIndex)
{
    if (sliceIndex < mSliceVector.Size() && mSliceVector[sliceIndex] != NULL)
    {
        INDEXLIB_THROW(misc::InconsistentStateException,
                       "Allocate slice fail, slice [%u] exist", sliceIndex);
    }
    if (sliceIndex > mSliceVector.Size())
    {
        INDEXLIB_THROW(misc::InconsistentStateException,
                       "Allocate slice fail, slice [%u], sliceNum [%lu]",
                       sliceIndex, mSliceVector.Size());
    }

    if (sliceIndex == mSliceVector.Size())
    {
        mSliceVector.Resize(sliceIndex + 1);
    }

    if (mPool != NULL) 
    {
        void *pAddr = mPool->allocate(mSliceLen * sizeof(T));
        mSliceVector[sliceIndex] = new(pAddr) T[mSliceLen];
    }
    else
    {
        mSliceVector[sliceIndex] = new T[mSliceLen];
    }

    if (mMetrics != NULL)
    {
        mMetrics->AddValue(sizeof(T) * mSliceLen);
    }
    
    return mSliceVector[sliceIndex];
}

template<class T>
T* SliceArray<T>::GetSlice(uint32_t sliceIndex)
{
    if (sliceIndex < mSliceVector.Size())
    {
        return mSliceVector[sliceIndex];
    }

    uint32_t i = mSliceVector.Size();
    mSliceVector.Resize(sliceIndex + 1);
    for (; i < mSliceVector.Size(); ++i)
    {
        AllocateSlice(i);
    }
    
    return mSliceVector[sliceIndex];
}

template<class T>
inline const T* SliceArray<T>::GetSlice(uint32_t sliceIndex) const
{
    assert(sliceIndex < mSliceVector.Size());
    return mSliceVector[sliceIndex];
}

template<class T>
T* SliceArray<T>::GetSliceWithoutAllocate(uint32_t sliceIndex)
{
    assert(sliceIndex < mSliceVector.Size());
    return mSliceVector[sliceIndex];
}

template<class T>
const T* SliceArray<T>::GetSliceAndZeroTail(uint32_t sliceIndex)
{
    assert(sliceIndex < mSliceVector.Size());
    if (sliceIndex < (mSliceVector.Size() - 1))
    {
        return mSliceVector[sliceIndex];
    }
    else
    {
        T* sliceData = mSliceVector[sliceIndex];
        uint32_t dataNum = (sliceIndex + 1) * mSliceLen;
        uint64_t validDataNum = (uint64_t)mMaxValidIndex + 1;
        if (dataNum > validDataNum)
        {
            memset(sliceData + (validDataNum % mSliceLen), 0, 
                   (dataNum - validDataNum) * sizeof(T));
        }
        return sliceData;
    }
}

template<class T>
inline const T& SliceArray<T>::operator[] (const int64_t index) const
{
    assert(index <= mMaxValidIndex);

    uint32_t sliceIndex = index / mSliceLen;
    uint32_t offset = index % mSliceLen;

    return (mSliceVector[sliceIndex])[offset];
}
template<class T>
inline void SliceArray<T>::Read(int64_t index, T* buf, uint32_t count) const
{
    uint32_t sliceIndex = index / mSliceLen;
    uint32_t offset = index % mSliceLen;
    uint32_t left = count;
    T* currentBuf = buf;
    
    while (true)
    {
        uint32_t currentLeft = mSliceLen - offset;
        uint32_t toCopy = left < currentLeft ? left : currentLeft;
        
        const T* slice = GetSlice(sliceIndex);
        memcpy((void*)currentBuf, (const void*)(slice + offset), toCopy * sizeof(T));
        currentBuf += toCopy;

        left -= toCopy;
        sliceIndex++;
        offset = 0;

        if (left == 0)
        {
            break;
        }
    }
}

template<class T>
inline void SliceArray<T>::Set(const int64_t index, const T& value)
{
    uint32_t sliceIndex = index / mSliceLen;
    uint32_t offset = index % mSliceLen;

    T* slice = GetSlice(sliceIndex);
    if (index > mMaxValidIndex)
    {
        mMaxValidIndex = index;
    }
    slice[offset] = value;
}

template<class T>
void SliceArray<T>::ReplaceSlice(int32_t sliceIndex, T* slice)
{
    if ((size_t)sliceIndex >= mSliceVector.Size())
    {
        std::stringstream ss;
        ss << "Replace slice FAIL, slice number = " << mSliceVector.Size() 
           << ", slice index = " << sliceIndex;
        INDEXLIB_THROW(misc::InconsistentStateException, "%s", ss.str().c_str());
    }

    memcpy(mSliceVector[sliceIndex], slice, sizeof(T) * mSliceLen);

    if (slice != NULL && (int32_t)((sliceIndex + 1) * mSliceLen - 1) > mMaxValidIndex)
    {
        mMaxValidIndex = (sliceIndex + 1) * mSliceLen - 1;
    }
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_SLICE_ARRAY_H
