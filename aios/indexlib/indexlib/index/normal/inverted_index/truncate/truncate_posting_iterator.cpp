#include "indexlib/index/normal/inverted_index/truncate/truncate_posting_iterator.h"

using namespace std;
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncatePostingIterator);

TruncatePostingIterator::TruncatePostingIterator(
        const PostingIteratorPtr& evaluatorIter, 
        const PostingIteratorPtr& collectorIter) 
    : mEvaluatorIter(evaluatorIter)
    , mCollectorIter(collectorIter)
    , mCurIter(evaluatorIter)
{
}

TruncatePostingIterator::~TruncatePostingIterator() 
{
}

TermMeta *TruncatePostingIterator::GetTermMeta() const
{
    return mCurIter->GetTermMeta();
}

docid_t TruncatePostingIterator::SeekDoc(docid_t docId)
{
    return mCurIter->SeekDoc(docId);
}

common::ErrorCode TruncatePostingIterator::SeekDocWithErrorCode(docid_t docId, docid_t& result)
{
    return mCurIter->SeekDocWithErrorCode(docId, result);
}

bool TruncatePostingIterator::HasPosition() const
{
    return mCurIter->HasPosition();
}

common::ErrorCode TruncatePostingIterator::SeekPositionWithErrorCode(pos_t pos, pos_t& result)
{
    return mCurIter->SeekPositionWithErrorCode(pos, result);
}

void TruncatePostingIterator::Unpack(TermMatchData& termMatchData)
{
    mCurIter->Unpack(termMatchData);
}

docpayload_t TruncatePostingIterator::GetDocPayload()
{
    return mCurIter->GetDocPayload();
}

PostingIteratorType TruncatePostingIterator::GetType() const
{
    return mCurIter->GetType();
}

void TruncatePostingIterator::Reset()
{
    mCurIter = mCollectorIter;
    mCurIter->Reset();
}

PostingIterator* TruncatePostingIterator::Clone() const
{
    PostingIteratorPtr iter1(mEvaluatorIter->Clone());
    PostingIteratorPtr iter2(mCollectorIter->Clone());
    TruncatePostingIterator* iter = new TruncatePostingIterator(iter1, iter2);
    return iter;
}

IE_NAMESPACE_END(index);

