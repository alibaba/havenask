#include "indexlib/index/normal/inverted_index/accessor/range_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/accessor/term_match_data.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_term_meta_calculator.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"

using namespace std;


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, RangePostingIterator);

RangePostingIterator::RangePostingIterator(const PostingFormatOption& postingFormatOption,
        autil::mem_pool::Pool* sessionPool)
    : PostingIteratorImpl(sessionPool)
    , mSegmentCursor(-1)
    , mCurrentDocId(INVALID_DOCID)
    , mSeekDocCounter(0)
    , mPostingFormatOption(postingFormatOption)
    , mSegmentPostingsIterator(postingFormatOption, mSeekDocCounter, sessionPool)
{
}

RangePostingIterator::~RangePostingIterator() 
{
}

bool RangePostingIterator::Init(const SegmentPostingsVec& segPostings)
{
    if (segPostings.empty())
    {
        return false;
    }
    mSegmentPostings = segPostings;

    MultiSegmentTermMetaCalculator currentChain;
    MultiSegmentTermMetaCalculator mainChain;    
    for (size_t i = 0; i < mSegmentPostings.size(); ++i)
    {
        SegmentPostingVector& postings = mSegmentPostings[i]->GetSegmentPostings();
        for (size_t j = 0; j < postings.size(); j++)
        {
            currentChain.AddSegment(postings[j].GetCurrentTermMeta());
            mainChain.AddSegment(postings[j].GetMainChainTermMeta());
        }
    }

    currentChain.InitTermMeta(mCurrentChainTermMeta);
    mainChain.InitTermMeta(mTermMeta);    
    return true;
}


index::TermPostingInfo RangePostingIterator::GetTermPostingInfo() const
{
    uint32_t postingSkipSize = 0;
    uint32_t postingListSize = 0;
    
    for (size_t i = 0; i < mSegmentPostings.size(); ++i)
    {
        SegmentPostingVector& postings = mSegmentPostings[i]->GetSegmentPostings();
        for (size_t j = 0; j < postings.size(); j++)
        {
            const util::ByteSliceListPtr& sliceListPtr = postings[i].GetSliceListPtr();
            const index::PostingFormatOption formatOption =
                postings[i].GetPostingFormatOption();

            index::TermMeta tm;
            common::ByteSliceReader reader(sliceListPtr.get());
            TermMetaLoader tmLoader(formatOption);
            tmLoader.Load(&reader, tm);

            index::TermPostingInfo postingInfo;
            postingInfo.LoadPosting(&reader);
            postingSkipSize += postingInfo.GetPostingSkipSize();
            postingListSize += postingInfo.GetPostingListSize();
        }
    }
    index::TermPostingInfo info;
    info.SetPostingSkipSize(postingSkipSize);
    info.SetPostingListSize(postingListSize);
    return info;
}

void RangePostingIterator::Reset()
{
    if (mSegmentPostings.size() == 0)
    {
        return;
    }
    mSegmentCursor = -1;
    mCurrentDocId = INVALID_DOCID;
    mSegmentPostingsIterator.Reset();
}

PostingIterator* RangePostingIterator::Clone() const
{
    RangePostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(mSessionPool,
            RangePostingIterator, mPostingFormatOption, mSessionPool);
    assert(iter != NULL);
    if (!iter->Init(mSegmentPostings))
    {
        IE_POOL_COMPATIBLE_DELETE_CLASS(mSessionPool, iter);
        iter = NULL;
    }
    return iter;
}

void RangePostingIterator::Unpack(TermMatchData& termMatchData)
{
    termMatchData.SetMatched(true);
}

IE_NAMESPACE_END(index);

