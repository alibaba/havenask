#include "indexlib/util/byte_slice_list/byte_slice_list.h"
#include "indexlib/misc/exception.h"
using namespace std;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);


////////////////////////////////////////////////////
IE_LOG_SETUP(util, ByteSliceList);

ByteSliceList::ByteSliceList()
{
    mHead = NULL;
    mTail = NULL;
    mTotalSize = 0;
}

ByteSliceList::ByteSliceList(ByteSlice* slice)
    : mHead(NULL)
    , mTail(NULL)
    , mTotalSize(0)
{
    if (slice != NULL)
    {
        Add(slice);
    }
}

ByteSliceList::~ByteSliceList()
{
    // we should call Clear() explicitly when using pool
    Clear(NULL);
    assert(!mHead);
    assert(!mTail);
}

void ByteSliceList::Add(ByteSlice* slice)
{
    if (mTail == NULL)
    {
        mHead = mTail = slice;
    }
    else
    {
        mTail->next = slice;
        mTail = slice;
    }
    mTotalSize += slice->size;
}
 
void ByteSliceList::Clear(Pool* pool)
{
    ByteSlice* slice = mHead;
    ByteSlice* next = NULL;

    while(slice)
    {
        next = slice->next;
        ByteSlice::DestroyObject(slice, pool);
        slice = next;
    }
    
    mHead = mTail = NULL;
    mTotalSize = 0;
}

size_t ByteSliceList::UpdateTotalSize()
{
    mTotalSize = 0;
    ByteSlice* slice = mHead;
    while(slice)
    {
        mTotalSize += slice->size;
        slice = slice->next;
    }
    return mTotalSize;
}

void ByteSliceList::MergeWith(ByteSliceList& other)
{
    //assert(this->mPool == other.mPool);

    if (mHead == NULL)    
    {
        mHead = other.mHead;
        mTail = other.mTail;
    }
    else
    {
        mTail->next = other.mHead;
        mTail = other.mTail;
    }

    mTotalSize += other.mTotalSize;
    other.mHead = other.mTail = NULL;
    other.mTotalSize = 0;
}

IE_NAMESPACE_END(util);
