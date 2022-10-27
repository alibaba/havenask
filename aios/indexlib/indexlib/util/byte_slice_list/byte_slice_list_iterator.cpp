#include "indexlib/util/byte_slice_list/byte_slice_list_iterator.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ByteSliceListIterator);

ByteSliceListIterator::ByteSliceListIterator(const ByteSliceList* sliceList)
    : mSliceList(sliceList)
    , mSlice(sliceList->GetHead())
    , mPosInSlice(0)
    , mSeekedSliceSize(0)
    , mEndPos(0)
{}

ByteSliceListIterator::ByteSliceListIterator(const ByteSliceListIterator& other)
    : mSliceList(other.mSliceList)
    , mSlice(other.mSlice)
    , mPosInSlice(other.mPosInSlice)
    , mSeekedSliceSize(other.mSeekedSliceSize)
    , mEndPos(other.mEndPos)
{}

bool ByteSliceListIterator::SeekSlice(size_t beginPos)
{
    if (beginPos < mSeekedSliceSize + mPosInSlice)
    {
        return false;
    }

    while (mSlice)
    {
        size_t sliceEndPos = mSeekedSliceSize + mSlice->size;
        if (beginPos >= mSeekedSliceSize && beginPos < sliceEndPos)
        {
            mPosInSlice = beginPos - mSeekedSliceSize;
            return true;
        }

        mSeekedSliceSize += mSlice->size;
        mPosInSlice = 0;
        mSlice = mSlice->next;
    }
    return false;
}

bool ByteSliceListIterator::HasNext(size_t endPos)
{
    if (mSlice == NULL || endPos > mSliceList->GetTotalSize())
    {
        return false;
    }

    mEndPos = endPos;
    size_t curPos = mSeekedSliceSize + mPosInSlice;
    return curPos < endPos;
}

void ByteSliceListIterator::Next(void* &data, size_t &size)
{
    assert(mSlice != NULL);

    size_t curPos = mSeekedSliceSize + mPosInSlice;
    size_t sliceEndPos = mSeekedSliceSize + mSlice->size;

    data = mSlice->data + mPosInSlice;
    if (mEndPos >= sliceEndPos)
    {
        size = sliceEndPos - curPos;
        mSeekedSliceSize += mSlice->size;
        mPosInSlice = 0;
        mSlice = mSlice->next;
    }
    else
    {
        size = mEndPos - curPos;
        mPosInSlice = mEndPos - mSeekedSliceSize;
    }
}

IE_NAMESPACE_END(util);

