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
#include "indexlib/index/inverted_index/RangePostingIterator.h"

#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/MultiSegmentTermMetaCalculator.h"
#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, RangePostingIterator);

RangePostingIterator::RangePostingIterator(const PostingFormatOption& postingFormatOption,
                                           autil::mem_pool::Pool* sessionPool, PostingIteratorImpl::TracerPtr tracer)
    : PostingIteratorImpl(sessionPool, std::move(tracer))
    , _segmentCursor(-1)
    , _currentDocId(INVALID_DOCID)
    , _seekDocCounter(0)
    , _postingFormatOption(postingFormatOption)
    , _segmentPostingsIterator(postingFormatOption, _seekDocCounter, sessionPool)
{
}

RangePostingIterator::~RangePostingIterator() {}

bool RangePostingIterator::Init(const SegmentPostingsVec& segPostings)
{
    if (segPostings.empty()) {
        return false;
    }
    _segmentPostings = segPostings;

    MultiSegmentTermMetaCalculator currentChain;
    MultiSegmentTermMetaCalculator mainChain;
    for (size_t i = 0; i < _segmentPostings.size(); ++i) {
        SegmentPostingVector& postings = _segmentPostings[i]->GetSegmentPostings();
        for (size_t j = 0; j < postings.size(); j++) {
            currentChain.AddSegment(postings[j].GetCurrentTermMeta());
            mainChain.AddSegment(postings[j].GetMainChainTermMeta());
        }
    }

    currentChain.InitTermMeta(_currentChainTermMeta);
    mainChain.InitTermMeta(_termMeta);
    return true;
}

TermPostingInfo RangePostingIterator::GetTermPostingInfo() const
{
    uint32_t postingSkipSize = 0;
    uint32_t postingListSize = 0;

    for (size_t i = 0; i < _segmentPostings.size(); ++i) {
        SegmentPostingVector& postings = _segmentPostings[i]->GetSegmentPostings();
        for (size_t j = 0; j < postings.size(); j++) {
            const PostingFormatOption formatOption = postings[i].GetPostingFormatOption();

            TermMeta tm;
            file_system::ByteSliceReader reader;
            util::ByteSlice* singleSlice = postings[i].GetSingleSlice();
            if (singleSlice) {
                reader.Open(singleSlice);
            } else {
                reader.Open(postings[i].GetSliceListPtr().get());
            }

            TermMetaLoader tmLoader(formatOption);
            tmLoader.Load(&reader, tm);

            TermPostingInfo postingInfo;
            postingInfo.LoadPosting(&reader);
            postingSkipSize += postingInfo.GetPostingSkipSize();
            postingListSize += postingInfo.GetPostingListSize();
        }
    }
    TermPostingInfo info;
    info.SetPostingSkipSize(postingSkipSize);
    info.SetPostingListSize(postingListSize);
    return info;
}

void RangePostingIterator::Reset()
{
    if (_segmentPostings.size() == 0) {
        return;
    }
    _segmentCursor = -1;
    _currentDocId = INVALID_DOCID;
    _segmentPostingsIterator.Reset();
}

PostingIterator* RangePostingIterator::Clone() const
{
    auto tracer = indexlib::util::MakePooledUniquePtr<PostingIteratorImpl::TracerPtr::ValueType>(_sessionPool);
    RangePostingIterator* iter = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, RangePostingIterator, _postingFormatOption,
                                                              _sessionPool, std::move(tracer));
    assert(iter != NULL);
    if (!iter->Init(_segmentPostings)) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, iter);
        iter = NULL;
    }
    return iter;
}

void RangePostingIterator::Unpack(TermMatchData& termMatchData) { termMatchData.SetMatched(true); }
} // namespace indexlib::index
