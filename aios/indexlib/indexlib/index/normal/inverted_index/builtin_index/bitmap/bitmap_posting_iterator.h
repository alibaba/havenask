#ifndef __INDEXLIB_BITMAP_POSTING_ITERATOR_H
#define __INDEXLIB_BITMAP_POSTING_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/position_iterator_typed.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/single_bitmap_posting_iterator.h"

IE_NAMESPACE_BEGIN(index);

class BitmapPostingIterator : 
    public PositionIteratorTyped<SingleBitmapPostingIterator>
{
public:
    BitmapPostingIterator(optionflag_t optionFlag = OPTION_FLAG_ALL,
                          autil::mem_pool::Pool *sessionPool = NULL);
    ~BitmapPostingIterator();

public:
    PostingIteratorType GetType() const override
    {
        return pi_bitmap;
    }

    PostingIterator* Clone() const override;
    bool HasPosition() const override { return false; }
public:
    bool Test(docid_t docId);
public:
    // for test
    docid_t GetCurrentGlobalDocId() 
    { return mSingleIterators[mSegmentCursor]->GetCurrentGlobalDocId(); }

protected:
    common::ErrorCode InitSingleIterator(SingleIteratorType *singleIterator) override;

protected:
    docid_t mCurBaseDocId;
    docid_t mCurLastDocId;
    docid_t mSegmentLastDocId;

private:
    friend class BitmapPostingIteratorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(BitmapPostingIterator);

////////////////////////////////////////////////////////

inline bool BitmapPostingIterator::Test(docid_t docId)
{
    while (true)
    {
        if (unlikely(docId < mCurBaseDocId))
        {
            return false;
        }
        if (docId < mCurLastDocId)
        {
            return mSingleIterators[mSegmentCursor]->Test(docId);
        }
        if (docId < mSegmentLastDocId)
        {
            return false;
        }
        if (!MoveToNextSegment().ValueOrThrow())
        {
            return false;
        }
    }
    return false;
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_POSTING_ITERATOR_H
