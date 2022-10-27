#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_DATA_ITERATER_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_DATA_ITERATER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/radix_tree.h"
#include "indexlib/common/typed_slice_list.h"

IE_NAMESPACE_BEGIN(index);

class VarNumAttributeDataIterator
{
    const static uint64_t INVALID_CURSOR = (uint64_t) -1;
public:
    VarNumAttributeDataIterator(
            common::RadixTree* data,
            common::TypedSliceList<uint64_t>* offsets)
        : mData(data)
        , mOffsets(offsets)
        , mCurrentData(NULL)
        , mDataLength(0)
        , mCurrentOffset(0)
        , mCursor(INVALID_CURSOR)
    {
    }

    ~VarNumAttributeDataIterator() 
    {
        mData = NULL;
        mOffsets = NULL;
    }

public:
    bool HasNext();
    void Next();
    void GetCurrentData(uint64_t& dataLength, uint8_t*& data);
    void GetCurrentOffset(uint64_t& offset);

private:
    common::RadixTree* mData;
    common::TypedSliceList<uint64_t>* mOffsets;
    uint8_t* mCurrentData;
    uint64_t mDataLength;
    uint64_t mCurrentOffset;
    uint64_t mCursor;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(VarNumAttributeDataIterator);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_DATA_ITERATER_H
