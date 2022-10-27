#ifndef __INDEXLIB_BYTES_ALIGNED_SLICE_ARRAY_H
#define __INDEXLIB_BYTES_ALIGNED_SLICE_ARRAY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include <indexlib/util/mmap_allocator.h>
#include "indexlib/util/slice_array/aligned_slice_array.h"
#include "indexlib/util/memory_control/simple_memory_quota_controller.h"

IE_NAMESPACE_BEGIN(util);

class SliceMemoryMetrics
{
public:
    SliceMemoryMetrics() {}
    virtual ~SliceMemoryMetrics() {}
public:
    virtual void Increase(size_t size) {}
    virtual void Decrease(size_t size) {}
};
DEFINE_SHARED_PTR(SliceMemoryMetrics);

//////////////////////////////////////////////////////////////////////
class BytesAlignedSliceArray
{
public:
    typedef std::vector<int64_t> SliceIdxVector;
    typedef AlignedSliceArray<char> SliceArray;

public:
    BytesAlignedSliceArray(const SimpleMemoryQuotaControllerPtr& memController,
                           uint64_t sliceLen, uint32_t maxSliceNum = 1);
    ~BytesAlignedSliceArray();

public:
    int64_t Insert(const void* value, size_t size);
    const uint64_t GetTotalUsedBytes() const { return mUsedBytes; }
    void* GetValueAddress(int64_t offset);

public:
    int64_t OffsetToSliceIdx(int64_t offset) const 
    { return mSliceArray.OffsetToSliceIdx(offset); }
    int64_t SliceIdxToOffset(int64_t sliceIdx) const
    { return mSliceArray.SliceIdxToOffset(sliceIdx); }

    int64_t GetSliceNum() const
    { return mSliceArray.GetSliceNum(); }
    uint64_t GetSliceLen() const
    { return mSliceArray.GetSliceLen(); }
    uint64_t GetCurrentOffset() const
    { return SliceIdxToOffset(mCurSliceIdx) + mCurCursor; }

    uint64_t GetSliceUsedBytes(int64_t sliceIdx) const
    { 
        if (sliceIdx < 0 || sliceIdx >= (int64_t)mSliceUsedSizeVector.size())
        {
            return 0;
        }
        return mSliceUsedSizeVector[sliceIdx]; 
    }
    int64_t GetCurrentSliceIdx() const { return mCurSliceIdx; }
    SliceIdxVector GetValidSliceIdxs() const;
    bool IsCurrentSliceEnough(size_t size);

    void AllocateSlice();
    void ReleaseSlice(int64_t sliceIdx);
    
    void SetSliceMemoryMetrics(const SliceMemoryMetricsPtr& storageMetrics)
    { mStorageMetrics = storageMetrics; }

private:
    int64_t FindValidSliceIdx();
    BytesAlignedSliceArray(const BytesAlignedSliceArray& src)
        : mSliceArray(src.mSliceArray)
    {}
 
private:
    SliceArray mSliceArray;
    SliceMemoryMetricsPtr mStorageMetrics;
    int32_t mMaxSliceNum;
    size_t mUsedBytes;
    uint64_t mCurCursor;
    int64_t mCurSliceIdx;
    std::vector<uint64_t> mSliceUsedSizeVector;
    SimpleMemoryQuotaControllerPtr mMemController;
private:
    IE_LOG_DECLARE();
    friend class BytesAlignedSliceArrayTest;
};

DEFINE_SHARED_PTR(BytesAlignedSliceArray);

inline bool BytesAlignedSliceArray::IsCurrentSliceEnough(size_t size)
{
    uint64_t sliceLen = GetSliceLen();
    if (mCurCursor == sliceLen || mCurCursor + size > sliceLen)
    {
        return false;
    }
    return true;
}

inline int64_t BytesAlignedSliceArray::Insert(
        const void* value, size_t size)
{
    assert(size <= GetSliceLen());
    if (!IsCurrentSliceEnough(size))
    {
        AllocateSlice();
    }

    if (size == 0)
    {
        return SliceIdxToOffset(mCurSliceIdx) + mCurCursor;
    }

    assert(value);
    void* ptr = mSliceArray.GetSliceWithoutAllocate(mCurSliceIdx) + mCurCursor;
    assert(ptr);
    memcpy(ptr, value, size);
    uint64_t cursor = mCurCursor;
    mCurCursor += size;
    mUsedBytes += size;
    mSliceUsedSizeVector[mCurSliceIdx] += size;
    mMemController->Allocate(size);
    if (mStorageMetrics)
    {
        mStorageMetrics->Increase(size);
    }
    return SliceIdxToOffset(mCurSliceIdx) + cursor;
}

inline int64_t BytesAlignedSliceArray::FindValidSliceIdx()
{
    int64_t sliceIdx = 0;
    for (; sliceIdx < (int64_t)mSliceArray.GetSliceNum(); sliceIdx++)
    {
        if (mSliceArray.GetSliceWithoutAllocate(sliceIdx) == NULL)
        {
            return sliceIdx;
        }
    }
    return sliceIdx;
}

inline void* BytesAlignedSliceArray::GetValueAddress(int64_t offset)
{
    return mSliceArray.OffsetToAddress(offset);
}

inline void BytesAlignedSliceArray::AllocateSlice()
{
    if (mCurSliceIdx + 1 >= mMaxSliceNum)
    {
        INDEXLIB_FATAL_ERROR(OutOfRange, "Slice index out of range,"
                             "curIndex[%ld], reservedSliceNum[%u]",
                             mCurSliceIdx, mMaxSliceNum);
    }        
    
    mCurSliceIdx = FindValidSliceIdx();
    mCurCursor = 0;
    mSliceArray.AllocateSlice(mCurSliceIdx);

    if (mCurSliceIdx >= (int64_t)mSliceUsedSizeVector.size())
    {
        mSliceUsedSizeVector.resize(mCurSliceIdx + 1);
    }
    mSliceUsedSizeVector[mCurSliceIdx] = 0;
}

inline void BytesAlignedSliceArray::ReleaseSlice(int64_t sliceIdx)
{
    if (sliceIdx < 0 || sliceIdx >= mSliceArray.GetSliceNum())
    {
        INDEXLIB_FATAL_ERROR(OutOfRange, "Release slice failed,"
                             "sliceIdx [%ld], sliceNum [%u]",
                             mCurSliceIdx, mSliceArray.GetSliceNum());
    }

    mSliceArray.ReleaseSlice(sliceIdx);
    
    uint64_t sliceUsedBytes = GetSliceUsedBytes(sliceIdx);
    mUsedBytes -= sliceUsedBytes;
    mMemController->Free(sliceUsedBytes);
    if (mStorageMetrics)
    {
        mStorageMetrics->Decrease(sliceUsedBytes);
    }
    mSliceUsedSizeVector[sliceIdx] = 0;
    if (mCurSliceIdx == sliceIdx)
    {
        mCurSliceIdx = -1;
        mCurCursor = mSliceArray.GetSliceLen() + 1;
    }
}

inline BytesAlignedSliceArray::SliceIdxVector
BytesAlignedSliceArray::GetValidSliceIdxs() const
{
    SliceIdxVector vec;
    for (uint32_t sliceIdx = 0; sliceIdx < mSliceArray.GetSliceNum(); sliceIdx++)
    {
        const void* slice = mSliceArray.GetSlice(sliceIdx);
        if (slice != NULL)
        {
            vec.push_back(sliceIdx);
        }
    }
    return vec;
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BYTES_ALIGNED_SLICE_ARRAY_H
