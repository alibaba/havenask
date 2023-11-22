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
#pragma once

#include "indexlib/base/Define.h"
#include "indexlib/index/inverted_index/PostingIteratorImpl.h"
#include "indexlib/index/inverted_index/RangeSegmentPostingsIterator.h"
#include "indexlib/index/inverted_index/SegmentPostings.h"

namespace indexlib::index {

class RangePostingIterator : public PostingIteratorImpl
{
public:
    RangePostingIterator(const PostingFormatOption& postingFormatOption, autil::mem_pool::Pool* sessionPool,
                         PostingIteratorImpl::TracerPtr tracer);
    ~RangePostingIterator();

    PostingIteratorType GetType() const override { return pi_range; }
    bool Init(const SegmentPostingsVec& segPostings);
    docid64_t SeekDoc(docid64_t docid) override { return InnerSeekDoc(docid); }
    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override;

    docpayload_t GetDocPayload() override { return 0; }
    pospayload_t GetPosPayload() { return 0; }
    bool HasPosition() const override { return false; }
    uint32_t GetSeekDocCount() const { return _seekDocCounter; }
    // TODO: support
    void Unpack(TermMatchData& termMatchData) override;
    PostingIterator* Clone() const override;
    void Reset() override;
    TermPostingInfo GetTermPostingInfo() const override;

    size_t GetPostingLength() const override
    {
        size_t totalSize = 0;
        for (auto& segPostingVec : _segmentPostings) {
            if (segPostingVec) {
                totalSize += PostingIteratorImpl::GetTotalPostingSize(segPostingVec->GetSegmentPostings());
            }
        }
        return totalSize;
    }

    docid64_t InnerSeekDoc(docid64_t docid);
    index::ErrorCode InnerSeekDoc(docid64_t docid, docid64_t& result);

private:
    uint32_t LocateSegment(uint32_t startSegCursor, docid64_t startDocId);
    bool MoveToSegment(docid64_t docid);
    docid64_t GetSegmentBaseDocId(uint32_t segmentCursor);

private:
    int32_t _segmentCursor;
    docid64_t _currentDocId;
    uint32_t _seekDocCounter;
    PostingFormatOption _postingFormatOption;
    SegmentPostingsVec _segmentPostings;
    RangeSegmentPostingsIterator _segmentPostingsIterator;

    AUTIL_LOG_DECLARE();
};

////////////////////////////////////////////////////////////////

inline index::ErrorCode RangePostingIterator::SeekDocWithErrorCode(docid64_t docId, docid64_t& result)
{
    return InnerSeekDoc(docId, result);
}

inline docid64_t RangePostingIterator::InnerSeekDoc(docid64_t docid)
{
    docid64_t ret = INVALID_DOCID;
    auto ec = InnerSeekDoc(docid, ret);
    index::ThrowIfError(ec);
    return ret;
}

__ALWAYS_INLINE inline index::ErrorCode RangePostingIterator::InnerSeekDoc(docid64_t docid, docid64_t& result)
{
    docid64_t curDocId = _currentDocId;
    docid = std::max(curDocId + 1, docid);
    while (true) {
        auto seekRet = _segmentPostingsIterator.Seek(docid);
        if (unlikely(!seekRet.Ok())) {
            return seekRet.GetErrorCode();
        }
        if (seekRet.Value()) {
            _currentDocId = _segmentPostingsIterator.GetCurrentDocid();
            result = _currentDocId;
            return index::ErrorCode::OK;
        }
        try {
            if (!MoveToSegment(docid)) {
                result = INVALID_DOCID;
                return index::ErrorCode::OK;
            }
        } catch (const util::FileIOException& e) {
            return index::ErrorCode::FileIO;
        }
    }
}

inline bool RangePostingIterator::MoveToSegment(docid64_t docid)
{
    if (_segmentCursor >= (int32_t)_segmentPostings.size()) {
        return false;
    }
    _segmentCursor = LocateSegment(_segmentCursor + 1, docid);
    if (_segmentCursor >= (int32_t)_segmentPostings.size()) {
        return false;
    }
    _segmentPostingsIterator.Init(_segmentPostings[_segmentCursor], docid,
                                  _segmentCursor + 1 < (int32_t)_segmentPostings.size()
                                      ? _segmentPostings[_segmentCursor + 1]->GetBaseDocId()
                                      : INVALID_DOCID);
    return true;
}

inline uint32_t RangePostingIterator::LocateSegment(uint32_t startSegCursor, docid64_t startDocId)
{
    uint32_t curSegCursor = startSegCursor;
    docid64_t nextSegBaseDocId = GetSegmentBaseDocId(curSegCursor + 1);
    while (nextSegBaseDocId != INVALID_DOCID && startDocId >= nextSegBaseDocId) {
        ++curSegCursor;
        nextSegBaseDocId = GetSegmentBaseDocId(curSegCursor + 1);
    }

    return curSegCursor;
}

inline docid64_t RangePostingIterator::GetSegmentBaseDocId(uint32_t segmentCursor)
{
    if (segmentCursor >= _segmentPostings.size()) {
        return INVALID_DOCID;
    }
    return _segmentPostings[segmentCursor]->GetBaseDocId();
}
} // namespace indexlib::index
