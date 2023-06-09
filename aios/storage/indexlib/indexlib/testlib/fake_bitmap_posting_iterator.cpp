/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
