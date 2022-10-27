#ifndef __INDEXLIB_BYTE_ALIGNED_SLICE_ARRAY_H
#define __INDEXLIB_BYTE_ALIGNED_SLICE_ARRAY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/slice_array/aligned_slice_array.h"

IE_NAMESPACE_BEGIN(util);
class ByteAlignedSliceArray : public AlignedSliceArray<char>
{
public:
    ByteAlignedSliceArray(uint32_t sliceLen, uint32_t initSliceNum = 1,
                          util::Metrics *metrics = NULL,
                          autil::mem_pool::PoolBase *pool = NULL,
                          bool isOwner = false);
    ByteAlignedSliceArray(const ByteAlignedSliceArray& src);
    ~ByteAlignedSliceArray();

public:
    template <typename T>
    inline void SetTypedValue(int64_t index, const T& value);

    template <typename T>
    inline void GetTypedValue(int64_t index, T& value);

    inline void SetByteList(int64_t index, char value, int64_t size);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ByteAlignedSliceArray);

////////////////////////////////////////////////////////////////////////
template<typename T>
inline void ByteAlignedSliceArray::SetTypedValue(int64_t index, const T& value)
{
    SetList(index, (char *)&value, sizeof(T));
}

template<typename T>
inline void ByteAlignedSliceArray::GetTypedValue(int64_t index, T& value)
{
    Read(index, (char *)&value, sizeof(T));
}

inline void ByteAlignedSliceArray::SetByteList(int64_t index, char value, int64_t size)
{
    uint32_t sliceIndex = index >> mPowerOf2;
    uint32_t offset     = index & mPowerMark;
    
    uint32_t sliceRemain = this->mSliceLen - offset;
    uint32_t toSetNum    = 0;
    int64_t  remainSize  = size;

    while(remainSize > 0)
    {
        value += toSetNum;
        char* slice = this->GetSlice(sliceIndex++);
        if (remainSize <= sliceRemain)
        {
            toSetNum = remainSize;
        }
        else
        {
            toSetNum = sliceRemain;
            sliceRemain = this->mSliceLen;
        }

        memset(slice + offset, value, toSetNum);
        remainSize -= toSetNum;
        offset = 0;
    }

    if ((index + size) > this->mMaxValidIndex)
    {
        this->mMaxValidIndex = index + size - 1;
    }
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BYTE_ALIGNED_SLICE_ARRAY_H
