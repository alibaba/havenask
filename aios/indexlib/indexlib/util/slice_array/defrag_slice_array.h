#ifndef __INDEXLIB_DEFRAG_SLICE_ARRAY_H
#define __INDEXLIB_DEFRAG_SLICE_ARRAY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/slice_array/bytes_aligned_slice_array.h"

IE_NAMESPACE_BEGIN(util);

class DefragSliceArray
{
public:
    struct SliceHeader
    {
        SliceHeader();
        int32_t wastedSize;
    };

    typedef BytesAlignedSliceArray SliceArray;
    typedef BytesAlignedSliceArrayPtr SliceArrayPtr;

public:
    DefragSliceArray(const SliceArrayPtr& sliceArray);
    virtual ~DefragSliceArray();

public:
    uint64_t Append(const void* value, size_t size);
    void Free(uint64_t offset, size_t size);
    const void* Get(uint64_t offset) const
    { return mSliceArray->GetValueAddress(offset); }

    static bool IsOverLength(size_t length, size_t sliceLen)
    {
        return (length + sizeof(SliceHeader)) > sliceLen; 
    }

protected:
    // wrapper for SliceArray
    int64_t GetCurrentSliceIdx() const
    { return mSliceArray->GetCurrentSliceIdx(); }
    int64_t SliceIdxToOffset(int64_t sliceIdx) const
    { return mSliceArray->SliceIdxToOffset(sliceIdx); }
    uint64_t GetSliceLen() const
    { return mSliceArray->GetSliceLen(); }
    void ReleaseSlice(int64_t sliceIdx)
    { mSliceArray->ReleaseSlice(sliceIdx); }    
    // wrapper for Header
    size_t GetHeaderSize() const
    { return sizeof(SliceHeader); }
    int32_t GetWastedSize(int64_t sliceIdx) const;
    void SetWastedSize(int64_t sliceIdx, int32_t wastedSize)
    { ((SliceHeader*)GetSlice(sliceIdx))->wastedSize = wastedSize; }

    //for test
    void IncreaseWastedSize(int64_t sliceIdx, int32_t wastedSize)
    { ((SliceHeader*)GetSlice(sliceIdx))->wastedSize += wastedSize; }

    int64_t GetSliceNum() const { return mSliceArray->GetSliceNum(); }

private:
    virtual void Defrag(int64_t sliceIdx) { }
    virtual void DoFree(size_t size) { }
    virtual bool NeedDefrag(int64_t sliceIdx) { return false; }

private:
    void AllocateNewSlice();
    void FreeLastSliceSpace(int64_t sliceIdx);
    void* GetSlice(int64_t sliceIdx) const;
    void FreeBySliceIdx(int64_t sliceIdx, size_t size);

private:
    SliceArrayPtr mSliceArray;

private:
    friend class DefragSliceArrayTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefragSliceArray);

////////////////////////////////////////////////////
inline void* DefragSliceArray::GetSlice(int64_t sliceIdx) const
{
    int64_t sliceOffset = mSliceArray->SliceIdxToOffset(sliceIdx);
    return mSliceArray->GetValueAddress(sliceOffset);
}

inline int32_t DefragSliceArray::GetWastedSize(int64_t sliceIdx) const
{
    SliceHeader* header = (SliceHeader*)GetSlice(sliceIdx);
    if (header)
    {
        return header->wastedSize;
    }
    return 0;
}
IE_NAMESPACE_END(util);

#endif //__INDEXLIB_DEFRAG_SLICE_ARRAY_H
