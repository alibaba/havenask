#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_sorted_queue_item.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);

BitmapIndexSortedQueueItem::BitmapIndexSortedQueueItem(
        const SegmentTermInfo& segTermInfo, 
        const ReclaimMapPtr& reclaimMap)
    : mBitmapDecoder(NULL)
    , mReclaimMap(reclaimMap)
    , mBaseDocId(segTermInfo.baseDocId)
    , mNewDocId(INVALID_DOCID)
    , mDecodedNum(0)
    , mDocCursor(0)
    , mEnd(false)
{
    mBitmapDecoder =         
        dynamic_cast<BitmapPostingDecoder*>(segTermInfo.postingDecoder);
    assert(mBitmapDecoder != NULL);
}

const index::TermMeta *BitmapIndexSortedQueueItem::GetTermMeta() const
{
    return mBitmapDecoder->GetTermMeta();
}

bool BitmapIndexSortedQueueItem::MoveNextInDocBuffer()
{
    while (mDocCursor < (uint32_t)mDecodedNum)
    {
        docid_t oldId = mDocBuffer[mDocCursor] + mBaseDocId;
        mDocCursor++;
        mNewDocId = mReclaimMap->GetNewId(oldId);
        if (mNewDocId != INVALID_DOCID)
        {
            return true;
        }
    }
    return false;
}

bool BitmapIndexSortedQueueItem::Next()
{
    if (mEnd)
    {
        return false;
    }

    if (MoveNextInDocBuffer())
    {
        return true;
    }
    
    while (1)
    {
        mDecodedNum = mBitmapDecoder->DecodeDocList(mDocBuffer, DEFAULT_BUFFER_SIZE);
        if (mDecodedNum <= 0)
        {
            mEnd = true;
            return false;
        }

        mDocCursor = 0;
        if (MoveNextInDocBuffer())
        {
            return true;
        }
    }
}

docid_t BitmapIndexSortedQueueItem::GetDocId() const
{
    return mNewDocId;
}

IE_NAMESPACE_END(index);

