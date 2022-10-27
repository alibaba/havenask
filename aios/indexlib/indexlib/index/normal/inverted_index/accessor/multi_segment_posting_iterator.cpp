#include "indexlib/index/normal/inverted_index/accessor/multi_segment_posting_iterator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiSegmentPostingIterator);

MultiSegmentPostingIterator::MultiSegmentPostingIterator()
    : mCursor(0)
{
}

MultiSegmentPostingIterator::~MultiSegmentPostingIterator() 
{
        //fileWriterPtr->Close(); //todo
}

common::ErrorCode MultiSegmentPostingIterator::SeekDocWithErrorCode(
    docid_t docId, docid_t& result) 
{
    if (unlikely(mCursor >= mSize))
    {
        result = INVALID_DOCID;
        return common::ErrorCode::OK;
    }
    while(mCursor < mSize)
    {
        if (mCursor + 1 < mSize && mPostingIterators[mCursor + 1].first <= docId)
        {
            ++mCursor;
            continue;
        }

        docid_t localDocid = docId - mPostingIterators[mCursor].first;
        auto errorCode = mPostingIterators[mCursor].second->SeekDocWithErrorCode(localDocid, result);
        if (errorCode != common::ErrorCode::OK)
        {
            return errorCode;
        }
            
        if (result != INVALID_DOCID)
        {
            return errorCode;
        }
 
        mCursor++;
    }
    result = INVALID_DOCID;
    return common::ErrorCode::OK;
}


docid_t MultiSegmentPostingIterator::SeekDoc(docid_t docId) 
{
    if (unlikely(mCursor >= mSize))
    {
        return INVALID_DOCID;
    }
    while(mCursor < mSize)
    {
        if (mCursor + 1 < mSize && mPostingIterators[mCursor + 1].first <= docId)
        {
            ++mCursor;
            continue;
        }

        docid_t localDocid = docId - mPostingIterators[mCursor].first;
        docid_t docid = mPostingIterators[mCursor].second->SeekDoc(localDocid);
        if (docid != INVALID_DOCID)
        {
            return mPostingIterators[mCursor].first + docid;
        }
        mCursor++;
    }
    return INVALID_DOCID;
}

void MultiSegmentPostingIterator::Init(
        std::vector<PostingIteratorInfo> postingIterators,
        std::vector<segmentid_t> segmentIdxs,
        std::vector<common::InMemFileWriterPtr> fileWriters)
{
    assert(postingIterators.size() == fileWriters.size());
    mPostingIterators = postingIterators;
    mSize = postingIterators.size();
    mFileWriters = fileWriters;
    mSegmentIdxs = segmentIdxs;
}

index::TermMeta* MultiSegmentPostingIterator::GetTermMeta() const
{
    assert(false);
    return mPostingIterators[mCursor].second->GetTermMeta();
}
index::TermMeta* MultiSegmentPostingIterator::GetTruncateTermMeta() const 
{
    assert(false);
    return NULL;
}
index::TermPostingInfo MultiSegmentPostingIterator::GetTermPostingInfo() const {
    assert(false);
    return mPostingIterators[mCursor].second->GetTermPostingInfo();
}
autil::mem_pool::Pool* MultiSegmentPostingIterator::GetSessionPool() const
{
    assert(false);
    return NULL;
    //return mPostingIterators[0]->GetSessionPool();
}


IE_NAMESPACE_END(index);

