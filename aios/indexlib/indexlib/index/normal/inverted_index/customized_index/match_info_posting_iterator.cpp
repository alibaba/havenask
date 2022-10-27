#include "indexlib/index/normal/inverted_index/customized_index/match_info_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MatchInfoPostingIterator);

MatchInfoPostingIterator::MatchInfoPostingIterator(
        const vector<SegmentMatchInfo>& segMatchInfos, 
        autil::mem_pool::Pool *sessionPool)
    : mSegMatchInfos(segMatchInfos)
    , mSessionPool(sessionPool)
    , mDocCursor(NULL)
    , mMatchValueCursor(NULL) 
    , mCurSegBaseDocId(INVALID_DOCID)
    , mCurrentDocId(INVALID_DOCID)
    , mCurMatchValue(matchvalue_t()) 
    , mSegCursor(-1)
    , mLastDocIdInCurSeg(INVALID_DOCID)
{
    df_t docFreq = 0;
    for (const auto& segMatchInfo: segMatchInfos)
    {
        docFreq += segMatchInfo.matchInfo->matchCount;
    }
    mTermMeta = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            index::TermMeta, docFreq, docFreq, 0);
}

MatchInfoPostingIterator::~MatchInfoPostingIterator() 
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, mTermMeta); 
}

index::TermMeta* MatchInfoPostingIterator::GetTermMeta() const
{
    return mTermMeta;
}

bool MatchInfoPostingIterator::HasPosition() const
{
    return false;
}

void MatchInfoPostingIterator::Unpack(TermMatchData& termMatchData)
{
    termMatchData.SetMatched(true);
}

void MatchInfoPostingIterator::Reset()
{
    mDocCursor = NULL;
    mMatchValueCursor = NULL;
    mCurSegBaseDocId = INVALID_DOCID;
    mCurrentDocId = INVALID_DOCID;
    mCurMatchValue = matchvalue_t();
    mSegCursor = -1;
    mLastDocIdInCurSeg = INVALID_DOCID;
}

PostingIterator* MatchInfoPostingIterator::Clone() const
{
    MatchInfoPostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(
            mSessionPool, MatchInfoPostingIterator,
            mSegMatchInfos, mSessionPool);
    return iter;
}

IE_NAMESPACE_END(index);

