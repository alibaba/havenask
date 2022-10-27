#include "indexlib/index/normal/primarykey/primary_key_posting_iterator.h"
#include "indexlib/index/normal/primarykey/primary_key_in_doc_position_state.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"

using namespace std;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PrimaryKeyPostingIterator);

PrimaryKeyPostingIterator::PrimaryKeyPostingIterator(docid_t docId,
        autil::mem_pool::Pool *sessionPool)
    : mDocId(docId)
    , mIsSearched(false)
    , mSessionPool(sessionPool)
{
    mTermMeta = new TermMeta(1, 1);
}

PrimaryKeyPostingIterator::~PrimaryKeyPostingIterator() 
{
    DELETE_AND_SET_NULL(mTermMeta);
}

TermMeta *PrimaryKeyPostingIterator::GetTermMeta() const
{
    return mTermMeta;
}

docid_t PrimaryKeyPostingIterator::SeekDoc(docid_t docId)
{
    if (mIsSearched)
    {
        return INVALID_DOCID;
    }
    else
    {
        mIsSearched = true;
        return (mDocId >= docId) ? mDocId : INVALID_DOCID;
    }
}

void PrimaryKeyPostingIterator::Unpack(TermMatchData& termMatchData)
{
    static PrimaryKeyInDocPositionState DUMMY_STATE;
    termMatchData.SetHasDocPayload(false);
    return termMatchData.SetInDocPositionState(&DUMMY_STATE);
}

PostingIterator* PrimaryKeyPostingIterator::Clone() const
{
    PrimaryKeyPostingIterator *iter = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            PrimaryKeyPostingIterator, mDocId, mSessionPool);
    iter->mIsSearched = false;
    return (PostingIterator *)iter;
}

void PrimaryKeyPostingIterator::Reset()
{
    mIsSearched = false;
}

IE_NAMESPACE_END(index);

