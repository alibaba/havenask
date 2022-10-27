
#include "indexlib/index/normal/inverted_index/accessor/posting_iterator_impl.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_segment_term_meta_calculator.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_loader.h"

using namespace std;

IE_NAMESPACE_USE(common);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, PostingIteratorImpl);

PostingIteratorImpl::PostingIteratorImpl(autil::mem_pool::Pool *sessionPool)
    : mSessionPool(sessionPool)
{}

PostingIteratorImpl::~PostingIteratorImpl()
{}

bool PostingIteratorImpl::Init(const SegmentPostingVectorPtr& segPostings)
{
    assert(segPostings);
    if (segPostings->size() == 0)
    {
        return false;
    }

    mSegmentPostings = segPostings;
    InitTermMeta();
    return true;
}

void PostingIteratorImpl::InitTermMeta()
{
    MultiSegmentTermMetaCalculator currentChain;
    MultiSegmentTermMetaCalculator mainChain;
    for (size_t i = 0; i < mSegmentPostings->size(); ++i)
    {
        currentChain.AddSegment((*mSegmentPostings)[i].GetCurrentTermMeta());
        mainChain.AddSegment((*mSegmentPostings)[i].GetMainChainTermMeta());
    }
    currentChain.InitTermMeta(mCurrentChainTermMeta);
    mainChain.InitTermMeta(mTermMeta);
}

index::TermMeta *PostingIteratorImpl::GetTermMeta() const
{
    return const_cast<index::TermMeta*>(&mTermMeta);
}

index::TermMeta *PostingIteratorImpl::GetTruncateTermMeta() const
{
    return const_cast<index::TermMeta*>(&mCurrentChainTermMeta);
}

index::TermPostingInfo PostingIteratorImpl::GetTermPostingInfo() const
{
    uint32_t postingSkipSize = 0;
    uint32_t postingListSize = 0;
    
    for (size_t i = 0; i < mSegmentPostings->size(); ++i)
    {
        const util::ByteSliceListPtr& sliceListPtr = (*mSegmentPostings)[i].GetSliceListPtr();
        const index::PostingFormatOption formatOption =
            (*mSegmentPostings)[i].GetPostingFormatOption();

        index::TermMeta tm;
        common::ByteSliceReader reader(sliceListPtr.get());
        TermMetaLoader tmLoader(formatOption);
        tmLoader.Load(&reader, tm);

        index::TermPostingInfo postingInfo;
        postingInfo.LoadPosting(&reader);
        postingSkipSize += postingInfo.GetPostingSkipSize();
        postingListSize += postingInfo.GetPostingListSize();
    }
    index::TermPostingInfo info;
    info.SetPostingSkipSize(postingSkipSize);
    info.SetPostingListSize(postingListSize);
    return info;
}

autil::mem_pool::Pool *PostingIteratorImpl::GetSessionPool() const
{
    return mSessionPool;
}

bool PostingIteratorImpl::operator == (const PostingIteratorImpl& right) const
{
    return mSessionPool == right.mSessionPool
        && mTermMeta  == right.mTermMeta
        && mCurrentChainTermMeta == right.mCurrentChainTermMeta
        && *mSegmentPostings == *right.mSegmentPostings;
}

IE_NAMESPACE_END(index);

