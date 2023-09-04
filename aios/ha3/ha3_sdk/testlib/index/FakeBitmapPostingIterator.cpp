#include "ha3_sdk/testlib/index/FakeBitmapPostingIterator.h"

#include <memory>
#include <vector>

#include "autil/Log.h"
#include "ha3_sdk/testlib/index/FakeTextIndexReader.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/SingleBitmapPostingIterator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Bitmap.h"
#include "indexlib/util/ObjectPool.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace indexlib {
namespace index {
AUTIL_LOG_SETUP(ha3, FakeBitmapPostingIterator);
using namespace indexlib::util;
using namespace indexlibv2::index;

FakeBitmapPostingIterator::FakeBitmapPostingIterator(const FakeTextIndexReader::RichPostings &p,
                                                     uint32_t statePoolSize,
                                                     autil::mem_pool::Pool *sessionPool)
    : BitmapPostingIterator(OPTION_FLAG_ALL, sessionPool)
    , _richPostings(p) {
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

FakeBitmapPostingIterator::~FakeBitmapPostingIterator() {
    _truncateTermMeta = NULL;
}

indexlib::index::ErrorCode
FakeBitmapPostingIterator::InitSingleIterator(SingleIteratorType *singleIter) {
    size_t docCount = _richPostings.second.size();
    Bitmap bitmap(BITMAP_SIZE);
    for (size_t i = 0; i < docCount; ++i) {
        bitmap.Set(_richPostings.second[i].docid);
    }
    singleIter->Init(bitmap, &_termMeta, &_statePool);
    singleIter->SetBaseDocId(0);
    _curLastDocId = BITMAP_SIZE - 1;
    _segmentLastDocId = BITMAP_SIZE - 1;
    return indexlib::index::ErrorCode::OK;
}

} // namespace index
} // namespace indexlib
