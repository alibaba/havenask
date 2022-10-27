#ifndef __INDEXLIB_ALIGNED_SLICE_ARRAY_H
#define __INDEXLIB_ALIGNED_SLICE_ARRAY_H

#include <tr1/memory>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/slice_array/slice_array.h"
#include "indexlib/util/metrics.h"

IE_NAMESPACE_BEGIN(util);

template<class T>
class AlignedSliceArray : public SliceArray<T>
{
public:
    AlignedSliceArray(uint64_t sliceLen, uint32_t initSliceNum = 1,
                      util::Metrics *metrics = NULL,
                      autil::mem_pool::PoolBase *pool = NULL,
                      bool isOwner = false);

    AlignedSliceArray(const AlignedSliceArray<T>& src);

    ~AlignedSliceArray();

public:
    inline const T& operator[] (int64_t offset) const;
    inline void Set(int64_t offset, const T& value);
    inline void SetList(int64_t offset, const T * value, int64_t size);
    inline void Read(int64_t offset, T* buf, uint32_t count) const;
    AlignedSliceArray* Clone() const;
    
    int64_t OffsetToSliceIdx(int64_t offset) const { return offset >> mPowerOf2; }
    int64_t SliceIdxToOffset(int64_t sliceIdx) const { return sliceIdx << mPowerOf2; }

    T* OffsetToAddress(int64_t offset)
    {
        size_t sliceIdx = offset >> mPowerOf2;
        size_t cursor = offset & mPowerMark;
        T* sliceAddress = this->GetSliceWithoutAllocate(sliceIdx);
        if (unlikely(sliceAddress == NULL))
        {
            return NULL;
        }
        return sliceAddress + cursor;
    }

protected:
    uint32_t mPowerOf2;
    uint32_t mPowerMark;

private:
    friend class AlignedSliceArrayTest;
};

typedef AlignedSliceArray<int32_t> Int32AlignedSliceArray;
typedef std::tr1::shared_ptr<Int32AlignedSliceArray> Int32AlignedSliceArrayPtr;

typedef AlignedSliceArray<uint32_t> UInt32AlignedSliceArray;
typedef std::tr1::shared_ptr<UInt32AlignedSliceArray> UInt32AlignedSliceArrayPtr;

////////////////////////////////////////////////////////////////////////

template<class T>
AlignedSliceArray<T>::AlignedSliceArray(uint64_t sliceLen, uint32_t initSliceNum,
                                        util::Metrics* metrics,
                                        autil::mem_pool::PoolBase* pool,
                                        bool isOwner)
    : SliceArray<T>(sliceLen, initSliceNum, metrics, pool, isOwner)
{
    assert((int64_t)sliceLen > 0 && initSliceNum > 0); // sliceLen must >=0 && <= 2^31
    // align mSliceLen
    this->mSliceLen = 1;
    mPowerOf2 = 0;
    while (this->mSliceLen < sliceLen)
    {
        this->mSliceLen <<= 1;
        mPowerOf2++;
    }
    mPowerMark = this->mSliceLen - 1;
}

template<class T>
AlignedSliceArray<T>::AlignedSliceArray(const AlignedSliceArray<T>& src)
    : SliceArray<T>(src)
    , mPowerOf2(src.mPowerOf2)
    , mPowerMark(src.mPowerMark)
{
}

template<class T>
AlignedSliceArray<T>::~AlignedSliceArray() 
{
}

template<class T>
inline const T& AlignedSliceArray<T>::operator[] (int64_t offset) const
{
    assert(offset <= this->mMaxValidIndex);

    int64_t sliceIndex = offset >> mPowerOf2;
    uint32_t sliceCursor = offset & mPowerMark;

    return (this->mSliceVector[sliceIndex])[sliceCursor];
}

template<class T>
inline void AlignedSliceArray<T>::Set(int64_t offset, const T& value)
{
    int64_t sliceIndex = offset >> mPowerOf2;
    uint32_t sliceCursor = offset & mPowerMark;

    T* slice = this->GetSlice(sliceIndex);
    slice[sliceCursor] = value;
    if (offset > this->mMaxValidIndex)
    {
        this->mMaxValidIndex = offset;
    }
}

template<class T>
inline void AlignedSliceArray<T>::SetList(int64_t offset, const T * value, int64_t size)
{
    int64_t sliceIndex = offset >> mPowerOf2;
    uint32_t sliceCursor     = offset & mPowerMark;
    
    uint64_t sliceRemain = this->mSliceLen - sliceCursor;
    uint64_t toSetNum    = 0;
    int64_t  remainSize  = size;

    while(remainSize > 0)
    {
        value += toSetNum;
        T* slice = this->GetSlice(sliceIndex++);
        if (remainSize <= (int64_t)sliceRemain)
        {
            toSetNum = remainSize;
        }
        else
        {
            toSetNum = sliceRemain;
            sliceRemain = this->mSliceLen;
        }
        memcpy(slice + sliceCursor, value, sizeof(T) * toSetNum);
        remainSize -= toSetNum;
        sliceCursor = 0;
    }

    if ((offset + size) > this->mMaxValidIndex)
    {
        this->mMaxValidIndex = offset + size - 1;
    }
}

template<class T>
inline void AlignedSliceArray<T>::Read(int64_t offset, T* buf, uint32_t count) const
{
    int64_t sliceIndex = offset >> mPowerOf2;
    uint32_t sliceCursor     = offset & mPowerMark;

    uint32_t left = count;
    T* currentBuf = buf;
    
    while (true)
    {
        uint32_t currentLeft = this->mSliceLen - sliceCursor;
        uint32_t toCopy = left < currentLeft ? left : currentLeft;
        
        const T* slice = this->GetSlice(sliceIndex++);
        memcpy((void*)currentBuf, (const void*)(slice + sliceCursor), toCopy * sizeof(T));
        currentBuf += toCopy;

        left -= toCopy;
        sliceCursor = 0;

        if (left == 0)
        {
            break;
        }
    }
}

template<class T>
AlignedSliceArray<T>* AlignedSliceArray<T>::Clone() const
{
    return new AlignedSliceArray<T>(*this);
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_ALIGNED_SLICE_ARRAY_H
