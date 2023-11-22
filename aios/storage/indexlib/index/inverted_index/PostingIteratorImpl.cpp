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
#include "indexlib/index/inverted_index/PostingIteratorImpl.h"

#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/MultiSegmentTermMetaCalculator.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PostingIteratorImpl);

PostingIteratorImpl::PostingIteratorImpl(autil::mem_pool::Pool* sessionPool, TracerPtr tracer)
    : _sessionPool(sessionPool)
    , _tracer(std::move(tracer))
{
}

PostingIteratorImpl::~PostingIteratorImpl() {}

bool PostingIteratorImpl::Init(const std::shared_ptr<SegmentPostingVector>& segPostings)
{
    assert(segPostings);
    if (segPostings->size() == 0) {
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
    for (size_t i = 0; i < mSegmentPostings->size(); ++i) {
        currentChain.AddSegment((*mSegmentPostings)[i].GetCurrentTermMeta());
        mainChain.AddSegment((*mSegmentPostings)[i].GetMainChainTermMeta());
    }
    currentChain.InitTermMeta(_currentChainTermMeta);
    mainChain.InitTermMeta(_termMeta);
}

TermMeta* PostingIteratorImpl::GetTermMeta() const { return const_cast<TermMeta*>(&_termMeta); }

TermMeta* PostingIteratorImpl::GetTruncateTermMeta() const { return const_cast<TermMeta*>(&_currentChainTermMeta); }

TermPostingInfo PostingIteratorImpl::GetTermPostingInfo() const
{
    uint32_t postingSkipSize = 0;
    uint32_t postingListSize = 0;

    for (size_t i = 0; i < mSegmentPostings->size(); ++i) {
        const SegmentPosting& segPosting = (*mSegmentPostings)[i];
        const PostingFormatOption formatOption = (*mSegmentPostings)[i].GetPostingFormatOption();

        TermMeta tm;
        file_system::ByteSliceReader reader;
        util::ByteSlice* singleSlice = segPosting.GetSingleSlice();
        if (singleSlice) {
            reader.Open(singleSlice).GetOrThrow();
        } else if (segPosting.GetSliceListPtr()) {
            reader.Open(segPosting.GetSliceListPtr().get()).GetOrThrow();
        } else {
            continue;
        }

        TermMetaLoader tmLoader(formatOption);
        tmLoader.Load(&reader, tm);
        TermPostingInfo postingInfo;
        postingInfo.LoadPosting(&reader);
        postingSkipSize += postingInfo.GetPostingSkipSize();
        postingListSize += postingInfo.GetPostingListSize();
    }
    TermPostingInfo info;
    info.SetPostingSkipSize(postingSkipSize);
    info.SetPostingListSize(postingListSize);
    return info;
}

autil::mem_pool::Pool* PostingIteratorImpl::GetSessionPool() const { return _sessionPool; }

bool PostingIteratorImpl::operator==(const PostingIteratorImpl& right) const
{
    return _sessionPool == right._sessionPool && _termMeta == right._termMeta &&
           _currentChainTermMeta == right._currentChainTermMeta && *mSegmentPostings == *right.mSegmentPostings;
}
} // namespace indexlib::index
