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
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"

#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingExpandData.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BitmapPostingIterator);

BitmapPostingIterator::BitmapPostingIterator(optionflag_t optionFlag, autil::mem_pool::Pool* sessionPool,
                                             indexlib::util::PooledUniquePtr<InvertedIndexSearchTracer> tracer)
    : PositionIteratorTyped<SingleBitmapPostingIterator>(optionFlag, sessionPool, std::move(tracer))
    , _curBaseDocId(INVALID_DOCID)
    , _curLastDocId(INVALID_DOCID)
    , _segmentLastDocId(INVALID_DOCID)
{
}

BitmapPostingIterator::~BitmapPostingIterator() {}

index::ErrorCode BitmapPostingIterator::InitSingleIterator(SingleIteratorType* singleIterator)
{
    const SegmentPosting& segPosting = (*mSegmentPostings)[_segmentCursor];
    _curBaseDocId = segPosting.GetBaseDocId();
    singleIterator->SetBaseDocId(_curBaseDocId);

    const PostingWriter* postingWriter = segPosting.GetInMemPostingWriter();
    BitmapPostingExpandData* expandData = nullptr;
    if (segPosting.GetResource()) {
        expandData = reinterpret_cast<BitmapPostingExpandData*>(segPosting.GetResource());
    }
    if (postingWriter) {
        // realtime segment
        singleIterator->Init(postingWriter, &_statePool);
        // realtime segment must be last segment
        _segmentLastDocId = singleIterator->GetLastDocId();
    } else {
        util::ByteSlice* singleSlice = segPosting.GetSingleSlice();
        if (singleSlice) {
            singleIterator->Init(singleSlice, expandData, &_statePool);
        } else if (segPosting.GetSliceListPtr()) {
            singleIterator->Init(segPosting.GetSliceListPtr(), expandData, &_statePool);
        } else {
            singleIterator->Init(expandData, &_statePool);
        }
        _segmentLastDocId = segPosting.GetBaseDocId() + segPosting.GetDocCount();
    }
    _curLastDocId = std::min(_segmentLastDocId, singleIterator->GetLastDocId());
    return index::ErrorCode::OK;
}

PostingIterator* BitmapPostingIterator::Clone() const
{
    BitmapPostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, BitmapPostingIterator, _optionFlag, _sessionPool);
    assert(iter != NULL);
    if (!iter->Init(mSegmentPostings, _statePool.GetElemCount())) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, iter);
        iter = NULL;
    }
    return iter;
}
} // namespace indexlib::index
