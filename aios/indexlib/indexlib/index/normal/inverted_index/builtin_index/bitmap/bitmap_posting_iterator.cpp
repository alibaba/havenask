
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_iterator.h"

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BitmapPostingIterator);

BitmapPostingIterator::BitmapPostingIterator(optionflag_t optionFlag,
                                             autil::mem_pool::Pool *sessionPool)
    : PositionIteratorTyped<SingleBitmapPostingIterator>(optionFlag, sessionPool)
    , mCurBaseDocId(INVALID_DOCID)
    , mCurLastDocId(INVALID_DOCID)
    , mSegmentLastDocId(INVALID_DOCID)
{
}

BitmapPostingIterator::~BitmapPostingIterator() 
{
}

common::ErrorCode BitmapPostingIterator::InitSingleIterator(SingleIteratorType *singleIterator)
{
    const SegmentPosting& segPosting = (*mSegmentPostings)[mSegmentCursor];
    mCurBaseDocId = segPosting.GetBaseDocId();
    singleIterator->SetBaseDocId(mCurBaseDocId);

    const PostingWriter *postingWriter = segPosting.GetInMemPostingWriter();
    if (postingWriter)
    {
        // realtime segment
        singleIterator->Init(postingWriter, &mStatePool);

        // realtime segment must be last segment
        mSegmentLastDocId = singleIterator->GetLastDocId();
    }
    else
    {
        singleIterator->Init(segPosting.GetSliceListPtr(), &mStatePool);
        mSegmentLastDocId = segPosting.GetBaseDocId() + segPosting.GetDocCount();
    }
    mCurLastDocId = std::min(mSegmentLastDocId, singleIterator->GetLastDocId());
    return common::ErrorCode::OK;
}

PostingIterator* BitmapPostingIterator::Clone() const
{
    BitmapPostingIterator *iter = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            BitmapPostingIterator, mOptionFlag, mSessionPool);
    assert(iter != NULL);
    if (!iter->Init(mSegmentPostings, mStatePool.GetElemCount()))
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, iter);
        iter = NULL;
    }
    return iter;
}

IE_NAMESPACE_END(index);

