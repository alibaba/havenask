#include "indexlib/index/normal/inverted_index/truncate/truncate_posting_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_posting_iterator.h"
#include "indexlib/index/normal/inverted_index/format/posting_format_option.h"

using namespace std;

IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, TruncatePostingIteratorCreator);

TruncatePostingIteratorCreator::TruncatePostingIteratorCreator() 
{
}

TruncatePostingIteratorCreator::~TruncatePostingIteratorCreator() 
{
}

PostingIteratorPtr TruncatePostingIteratorCreator::Create(
        const IndexConfigPtr& indexConfig,
        const SegmentPosting& segPosting,
        bool isBitmap) 
{
    SegmentPostingVectorPtr segPostings(new SegmentPostingVector);
    segPostings->push_back(segPosting);

    PostingIteratorPtr iter1 = CreatePostingIterator(indexConfig, segPostings, isBitmap);
    PostingIteratorPtr iter2 = CreatePostingIterator(indexConfig, segPostings, isBitmap);
    TruncatePostingIterator *iter = new TruncatePostingIterator(iter1, iter2);
    return PostingIteratorPtr(iter);
}

PostingIteratorPtr TruncatePostingIteratorCreator::Create(
        const PostingIteratorPtr& it)
{
    PostingIteratorPtr clonedIt(it->Clone());

    TruncatePostingIterator *iter = new TruncatePostingIterator(it, clonedIt);
    return PostingIteratorPtr(iter);
}

PostingIteratorPtr TruncatePostingIteratorCreator::CreatePostingIterator(
        const IndexConfigPtr& indexConfig,
        const SegmentPostingVectorPtr& segPostings, 
        bool isBitmap)
{
    if (isBitmap)
    {
        return CreateBitmapPostingIterator(indexConfig->GetOptionFlag(),
                segPostings);
    }

    PostingFormatOption postingFormatOption;
    postingFormatOption.Init(indexConfig);
    BufferedPostingIterator *iter = 
        new BufferedPostingIterator(postingFormatOption, NULL);
    iter->Init(segPostings, NULL, 1);
    return PostingIteratorPtr(iter);
}

PostingIteratorPtr 
TruncatePostingIteratorCreator::CreateBitmapPostingIterator(
        optionflag_t optionFlag, const SegmentPostingVectorPtr& segPostings)
{
    BitmapPostingIterator* iter = new BitmapPostingIterator(optionFlag);
    iter->Init(segPostings, 1);
    return PostingIteratorPtr(iter);
}

IE_NAMESPACE_END(index);

