#include <ha3_sdk/testlib/index/FakeBitmapPostingIterator.h>
#include <indexlib/index/normal/inverted_index/builtin_index/bitmap/single_bitmap_posting_iterator.h>
#include <indexlib/util/bitmap.h>

IE_NAMESPACE_BEGIN(index);
HA3_LOG_SETUP(index, FakeBitmapPostingIterator);
IE_NAMESPACE_USE(inverted_index);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);

FakeBitmapPostingIterator::FakeBitmapPostingIterator(
        const FakeTextIndexReader::RichPostings &p,
        uint32_t statePoolSize,
        autil::mem_pool::Pool *sessionPool)
    : BitmapPostingIterator(OPTION_FLAG_ALL, sessionPool)
    , _richPostings(p){
    mStatePool.Init(statePoolSize);
    mSegmentCursor = -1;
    size_t docCount = _richPostings.second.size();
    mTermMeta = TermMeta(docCount, 0, _richPostings.first);
    _truncateTermMeta = &mTermMeta;

    mSegmentPostings.reset(new SegmentPostingVector);
    PostingFormatOption option(mOptionFlag);
    SegmentPosting segPosting(option);
    mSegmentPostings->push_back(segPosting);

    (void)MoveToNextSegment();
}

FakeBitmapPostingIterator::~FakeBitmapPostingIterator() {
    _truncateTermMeta = NULL;
}

IE_NAMESPACE(common)::ErrorCode FakeBitmapPostingIterator::InitSingleIterator(
        SingleIteratorType *singleIter)
{
    size_t docCount = _richPostings.second.size();
    Bitmap bitmap(BITMAP_SIZE);
    for (size_t i = 0; i < docCount; ++i) {
        bitmap.Set(_richPostings.second[i].docid);
    }
    singleIter->Init(bitmap, &mTermMeta, &mStatePool);
    singleIter->SetBaseDocId(0);
    mCurLastDocId = BITMAP_SIZE - 1;
    mSegmentLastDocId = BITMAP_SIZE - 1;
    return IE_NAMESPACE(common)::ErrorCode::OK;
}

IE_NAMESPACE_END(index);
