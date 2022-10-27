#include "indexlib/index/normal/attribute/accessor/var_num_attribute_data_iterator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributeDataIterator);

bool VarNumAttributeDataIterator::HasNext()
{
    if (mCursor == INVALID_CURSOR)
    {
        return mOffsets->Size() > 0;
    }

    return mCursor < mOffsets->Size() - 1;
}

void VarNumAttributeDataIterator::Next()
{
    if (mCursor == INVALID_CURSOR)
    {
        mCursor = 0;
    }
    else
    {
        mCursor++;
    }

    if (mCursor >= mOffsets->Size())
    {
        return;
    }

    mCurrentOffset = (*mOffsets)[mCursor];
    uint8_t* buffer = mData->Search(mCurrentOffset);
    mDataLength = *(uint32_t*)buffer;
    mCurrentData = buffer + sizeof(uint32_t);
}

void VarNumAttributeDataIterator::GetCurrentData(
        uint64_t& dataLength, uint8_t*& data)
{
    if (mCursor == INVALID_CURSOR || mCursor == mOffsets->Size())
    {
        IE_LOG(ERROR, "current cursor is invalid. [%lu]", mCursor);
        data = NULL;
        dataLength = 0;
        return;
    }
    dataLength = mDataLength;
    data = mCurrentData;
}

void VarNumAttributeDataIterator::GetCurrentOffset(uint64_t& offset)
{
    if (mCursor == INVALID_CURSOR || mCursor == mOffsets->Size())
    {
        IE_LOG(ERROR, "current cursor is invalid. [%lu]", mCursor);
        offset = 0;
        return;
    }
    offset = mCurrentOffset;
}

IE_NAMESPACE_END(index);
