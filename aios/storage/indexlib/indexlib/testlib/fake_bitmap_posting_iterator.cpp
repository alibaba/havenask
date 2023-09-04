#include "indexlib/testlib/fake_bitmap_posting_iterator.h"

#include "indexlib/index/inverted_index/builtin_index/bitmap/SingleBitmapPostingIterator.h"
#include "indexlib/util/Bitmap.h"

using namespace std;
using namespace indexlib::index;
using namespace indexlib::util;

namespace indexlib { namespace testlib {
IE_LOG_SETUP(testlib, FakeBitmapPostingIterator);

FakeBitmapPostingIterator::FakeBitmapPostingIterator(const FakePostingIterator::RichPostings& p, uint32_t statePoolSize,
                                                     autil::mem_pool::Pool* sessionPool)
    : BitmapPostingIterator(OPTION_FLAG_ALL, sessionPool)
    , _richPostings(p)
{
    _statePool.Init(statePoolSize);
    _segmentCursor = -1;
    size_t docCount = _richPostings.second.size();
    _termMeta = TermMeta(docCount, 0, _richPostings.first);
    _truncateTermMeta = &_termMeta;

    mSegmentPostings.reset(new SegmentPostingVector);
    PostingFormatOption option(_optionFlag);
    SegmentPosting segPosting(option);
    mSegmentPostings->push_back(segPosting);

    (void)MoveToNextSegment();
}

FakeBitmapPostingIterator::~FakeBitmapPostingIterator() { _truncateTermMeta = NULL; }

ErrorCode FakeBitmapPostingIterator::InitSingleIterator(SingleIteratorType* singleIter)
{
    size_t docCount = _richPostings.second.size();
    Bitmap bitmap(BITMAP_SIZE);
    for (size_t i = 0; i < docCount; ++i) {
        bitmap.Set(_richPostings.second[i].docid);
    }
    singleIter->Init(bitmap, &_termMeta, &_statePool);
    singleIter->SetBaseDocId(0);
    _curLastDocId = BITMAP_SIZE - 1;
    _segmentLastDocId = BITMAP_SIZE - 1;
    return ErrorCode::OK;
}
}} // namespace indexlib::testlib
