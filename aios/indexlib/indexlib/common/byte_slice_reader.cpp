#include <string.h>
#include <assert.h>
#include <vector>
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/common/byte_slice_reader.h"
#include "indexlib/misc/exception.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(common);

ByteSliceReader::ByteSliceReader()
    : mCurrentSlice(NULL)
    , mCurrentSliceOffset(0)
    , mGlobalOffset(0)
    , mSliceList(nullptr)
    , mSize(0)
    , mIsBlockByteSliceList(false)
{
}

ByteSliceReader::ByteSliceReader(ByteSliceList* sliceList)
    : ByteSliceReader()
{
    Open(sliceList);
}

void ByteSliceReader::Open(ByteSliceList* sliceList)
{
    Close();
    mSliceList = sliceList;
    mSize = GetSize();

    auto headSlice = mSliceList->GetHead();
    if (sliceList->getObjectType() ==
        IE_SUB_CLASS_TYPE_NAME(ByteSliceList, BlockByteSliceList))
    {
        mIsBlockByteSliceList = true;
        headSlice = PrepareSlice(headSlice);
    }
    Open(headSlice);
}

void ByteSliceReader::Open(ByteSlice* slice)
{
    mCurrentSlice = slice;
    mGlobalOffset = 0;
    mCurrentSliceOffset = 0;

    if (mCurrentSlice == NULL)
    {
        HandleError("Read past EOF.");
    }
}

void ByteSliceReader::Close()
{
    mCurrentSlice = NULL;
    mGlobalOffset = 0;
    mCurrentSliceOffset = 0;
    mSize = 0;
    mIsBlockByteSliceList = false;
    if (mSliceList)
    {
        mSliceList = NULL;
    }    
}

size_t ByteSliceReader::Read(void* value, size_t len)
{
    if (mCurrentSlice == NULL || len == 0)
    {
        return 0;
    }

    if (mCurrentSliceOffset + len <= GetSliceDataSize(mCurrentSlice))
    {
        memcpy(value, mCurrentSlice->data + mCurrentSliceOffset, len);
        mCurrentSliceOffset += len;
        mGlobalOffset += len;
        return len;
    }
    // current byteslice is not long enough, read next byteslices
    char* dest = (char*)value;
    int64_t totalLen = (int64_t)len;
    size_t offset = mCurrentSliceOffset;
    int64_t leftLen = 0;
    while (totalLen > 0)
    {
        leftLen = GetSliceDataSize(mCurrentSlice) - offset;
        if (leftLen < totalLen)
        {
            memcpy(dest, mCurrentSlice->data + offset, leftLen);
            totalLen -= leftLen;
            dest += leftLen;

            offset = 0;
            mCurrentSlice = NextSlice(mCurrentSlice);
            if (!mCurrentSlice)
            {
                break;
            }
        }
        else
        {
            memcpy(dest, mCurrentSlice->data + offset, totalLen);
            dest += totalLen;
            offset += (size_t)totalLen;
            totalLen = 0;
        }
    }
        
    mCurrentSliceOffset = offset;
    size_t readLen = (size_t)(len - totalLen);
    mGlobalOffset += readLen;
    return readLen;
}

size_t ByteSliceReader::ReadMayCopy(void*& value, size_t len)
{
    if (mCurrentSlice == NULL || len == 0)
        return 0;

    if (mCurrentSliceOffset + len <= GetSliceDataSize(mCurrentSlice))
    {
        value = mCurrentSlice->data + mCurrentSliceOffset;
        mCurrentSliceOffset += len;
        mGlobalOffset += len;
        return len;
    }
    return Read(value, len);
}

size_t ByteSliceReader::Seek(size_t offset)
{
    if (offset < mGlobalOffset)
    {
        stringstream ss;
        ss << "Invalid offset value: seek offset = " << offset;
        HandleError(ss.str());
    }

    size_t len = offset - mGlobalOffset;
    if (mCurrentSlice == NULL || len == 0)
    {
        return mGlobalOffset;
    }

    if (mCurrentSliceOffset + len < GetSliceDataSize(mCurrentSlice))
    {
        mCurrentSliceOffset += len;
        mGlobalOffset += len;
        return mGlobalOffset;
    }
    else 
    {
        // current byteslice is not long enough, seek to next byteslices
        int64_t totalLen = len;
        int64_t remainLen;
        while (totalLen > 0)
        {
            remainLen = mCurrentSlice->size - mCurrentSliceOffset;
            if (remainLen <= totalLen)
            {
                mGlobalOffset += remainLen;
                totalLen -= remainLen;
                mCurrentSlice = mCurrentSlice->next;
                mCurrentSliceOffset = 0;
                if (mCurrentSlice == NULL)
                {
                    if (mGlobalOffset == GetSize())
                    {
                        mGlobalOffset = BYTE_SLICE_EOF;
                    }
                    return BYTE_SLICE_EOF;
                }
            }
            else
            {
                mGlobalOffset += totalLen;
                mCurrentSliceOffset += totalLen;
                totalLen = 0;
            }
        }
        if (mGlobalOffset == GetSize())
        {
            mGlobalOffset = BYTE_SLICE_EOF;
            mCurrentSlice = ByteSlice::GetImmutableEmptyObject();
            mCurrentSliceOffset = 0;
        }
        else
        {
            mCurrentSlice = PrepareSlice(mCurrentSlice);
        }
        return mGlobalOffset;
    }
}

void ByteSliceReader::HandleError(const std::string& msg) const
{
    stringstream ss;
    ss << msg << ", State: list length = " << GetSize() 
       << ", offset = " << mGlobalOffset;
    INDEXLIB_THROW(OutOfRangeException, "%s", ss.str().c_str());
}

IE_NAMESPACE_END(common);

