#include "indexlib/index/normal/inverted_index/accessor/seek_and_filter_iterator.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, SeekAndFilterIterator);

SeekAndFilterIterator::SeekAndFilterIterator(
        PostingIterator* indexSeekIterator,
        DocValueFilter* docFilter,
        autil::mem_pool::Pool *sessionPool)
    : mIndexSeekIterator(indexSeekIterator)
    , mDocFilter(docFilter)
    , mSessionPool(sessionPool)
    , mNeedInnerFilter(false)
{
    assert(mIndexSeekIterator);

    // currently: only spatial index not recall precisely, (date ?)
    mNeedInnerFilter = (indexSeekIterator->GetType() == pi_spatial);
}

SeekAndFilterIterator::~SeekAndFilterIterator()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mIndexSeekIterator);
    if (mDocFilter)
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mDocFilter);
    }
}

PostingIterator* SeekAndFilterIterator::Clone() const
{
    assert(mIndexSeekIterator);
    SeekAndFilterIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(
            mSessionPool, SeekAndFilterIterator,
            mIndexSeekIterator->Clone(),
            mDocFilter ? mDocFilter->Clone() : NULL, mSessionPool);
    assert(iter);
    return iter;
}

docid_t SeekAndFilterIterator::SeekDoc(docid_t docid)
{
    if (!mNeedInnerFilter || !mDocFilter)
    {
        return mIndexSeekIterator->SeekDoc(docid);
    }
    docid_t curDocId = docid;
    while (true)
    {
        curDocId = mIndexSeekIterator->SeekDoc(curDocId);
        if (curDocId == INVALID_DOCID)
        {
            break;
        }
        if (mDocFilter->Test(curDocId))
        {
            return curDocId;
        }
        ++curDocId;
    }
    return INVALID_DOCID;
}

common::ErrorCode SeekAndFilterIterator::SeekDocWithErrorCode(docid_t docId, docid_t& result) {
{
    if (!mNeedInnerFilter || !mDocFilter)
    {
        return mIndexSeekIterator->SeekDocWithErrorCode(docId, result);
    }
    docid_t curDocId = docId;
    while (true)
    {
        auto ec = mIndexSeekIterator->SeekDocWithErrorCode(curDocId, curDocId);
        if (unlikely(ec != common::ErrorCode::OK))
        {
            return ec;
        }
        if (curDocId == INVALID_DOCID)
        {
            break;
        }
        if (mDocFilter->Test(curDocId))
        {
            result = curDocId;
            return common::ErrorCode::OK;
        }
        ++curDocId;
    }
    result = INVALID_DOCID;
    return common::ErrorCode::OK;
}

}

IE_NAMESPACE_END(index);

