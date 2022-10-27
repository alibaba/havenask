#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/misc/exception.h"
#include <string.h>
#include <assert.h>

using namespace std;
using namespace autil::mem_pool;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ByteSliceWriter);

ByteSliceWriter::ByteSliceWriter(Pool *pool, uint32_t minSliceSize)
    : mPool(pool)
    , mLastSliceSize(minSliceSize - ByteSlice::GetHeadSize())
    , mIsOwnSliceList(true)
#ifdef PACK_INDEX_MEMORY_STAT
    , mAllocatedSize(0)
#endif
{
    mSliceList = AllocateByteSliceList();
    mSliceList->Add(CreateSlice(mLastSliceSize));//Add the first slice
}

ByteSliceWriter::~ByteSliceWriter() 
{
    Close();
}

ByteSlice* ByteSliceWriter::CreateSlice(uint32_t size)
{
    ByteSlice* slice = ByteSlice::CreateObject(size, mPool);

#ifdef PACK_INDEX_MEMORY_STAT
    mAllocatedSize += size + ByteSlice::GetHeadSize();
#endif

    mLastSliceSize = slice->size;
    slice->size = 0;
    return slice;
}

size_t ByteSliceWriter::GetSize() const
{
    return size_t(mSliceList->GetTotalSize());
}

void ByteSliceWriter::Dump(FileWriter* file)
{
    assert(mSliceList != NULL);

    ByteSlice* slice = mSliceList->GetHead();
    while (slice != NULL)
    {
        file->Write((void*)(slice->data), slice->size);
        slice = slice->next;
    }
}

void ByteSliceWriter::Dump(const file_system::FileWriterPtr& file)
{
    assert(mSliceList != NULL);

    ByteSlice* slice = mSliceList->GetHead();
    while (slice != NULL)
    {
        file->Write((void*)(slice->data), slice->size);
        slice = slice->next;
    }
}

void ByteSliceWriter::Reset()
{
    mLastSliceSize = MIN_SLICE_SIZE - ByteSlice::GetHeadSize();
    if (mIsOwnSliceList && mSliceList)
    {
        mSliceList->Clear(mPool);
        DeAllocateByteSliceList(mSliceList);
    }
    mSliceList = AllocateByteSliceList();
    mSliceList->Add(CreateSlice(mLastSliceSize));//Add the first slice
}

void ByteSliceWriter::Close()
{
    if (mIsOwnSliceList && mSliceList)
    {
        mSliceList->Clear(mPool);
        DeAllocateByteSliceList(mSliceList);
    }
}

void ByteSliceWriter::Write(const void* value, size_t len)
{
    uint32_t left = (uint32_t)len;
    uint8_t* data = (uint8_t*)(value);
    ByteSlice* slice = mSliceList->GetTail();
    while (left > 0)
    {
        if (slice->size >= mLastSliceSize)
        {
            mLastSliceSize = GetIncrementedSliceSize(mLastSliceSize);
            slice = CreateSlice(mLastSliceSize);
            mSliceList->Add(slice);
        }
        uint32_t nCopyLen = (mLastSliceSize - slice->size) > left ? 
                            left : (mLastSliceSize - slice->size);
        memcpy(slice->data + slice->size, data, nCopyLen);
        data += nCopyLen;
        left -= nCopyLen;
        slice->size += nCopyLen;
    }
    mSliceList->IncrementTotalSize(len);
}

void ByteSliceWriter::Write(ByteSliceList& src)
{
    mSliceList->MergeWith(src);
}

void ByteSliceWriter::Write(const ByteSliceList& src, uint32_t start, uint32_t end)
{
    if (start >= end || end > src.GetTotalSize())
    {
        char msg[50] = {0};
        snprintf(msg, 50, "start = %u, end = %u, totalSize = %lu", 
                     start, end, src.GetTotalSize());
        INDEXLIB_THROW(misc::OutOfRangeException, "%s", msg);
    }

    // find start slice
    ByteSlice* currSlice = NULL;
    ByteSlice* nextSlice = src.GetHead();
    uint32_t nextSliceOffset = 0;
    while (start >= nextSliceOffset)
    {
        currSlice = nextSlice;
        nextSlice = currSlice->next;
        nextSliceOffset += currSlice->size;
    }

    // move and copy data, until reach end slice
    while (end > nextSliceOffset)
    {
        // copy src[start, nextSliceOffset) to *this
        uint32_t copyLen = nextSliceOffset - start;
        Write(currSlice->data + currSlice->size - copyLen, copyLen);

        // move to next slice, update var start
        currSlice = nextSlice;
        nextSlice = currSlice->next;
        start = nextSliceOffset;
        nextSliceOffset += currSlice->size;
    }
    // copy src[start, end) to *this
    uint32_t copyLen = end - start;
    uint32_t currSliceOffset = nextSliceOffset - currSlice->size;
    Write(currSlice->data + start - currSliceOffset, copyLen);
}

void ByteSliceWriter::SnapShot(ByteSliceWriter& byteSliceWriter) const 
{
    byteSliceWriter.Close();
    byteSliceWriter.mSliceList = mSliceList;
    byteSliceWriter.mLastSliceSize = mLastSliceSize;
    byteSliceWriter.mIsOwnSliceList = false;

#ifdef PACK_INDEX_MEMORY_STAT
    byteSliceWriter.mAllocatedSize = mAllocatedSize;
#endif
}

IE_NAMESPACE_END(common);

