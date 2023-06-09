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
#include <memory>

#include "indexlib/file_system/file/InterimFileWriter.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/PostingIteratorImpl.h"

namespace indexlib::index {

class MultiSegmentPostingIterator : public PostingIterator
{
public:
    using PostingIteratorInfo = std::pair<int64_t, std::shared_ptr<PostingIterator>>;

    MultiSegmentPostingIterator();
    ~MultiSegmentPostingIterator();

    void Init(std::vector<PostingIteratorInfo> postingIterators, std::vector<segmentid_t> segmentIdxs,
              std::vector<file_system::InterimFileWriterPtr> fileWriters);

    size_t GetSegmentPostingCount() const { return _size; }
    std::shared_ptr<PostingIterator> GetSegmentPostingIter(size_t idx, segmentid_t& segmentIdx)
    {
        segmentIdx = _segmentIdxs[idx];
        return _postingIterators[idx].second;
    }

    TermMeta* GetTermMeta() const override;
    TermMeta* GetTruncateTermMeta() const override;
    TermPostingInfo GetTermPostingInfo() const override;
    autil::mem_pool::Pool* GetSessionPool() const override;

    docid_t SeekDoc(docid_t docId) override;
    index::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;

    bool HasPosition() const override { return _postingIterators[_cursor].second->HasPosition(); }
    void Unpack(TermMatchData& termMatchData) override { _postingIterators[_cursor].second->Unpack(termMatchData); }

    docpayload_t GetDocPayload() override { return _postingIterators[_cursor].second->GetDocPayload(); }

    PostingIteratorType GetType() const override
    {
        assert(false);
        return pi_unknown;
    }

    void Reset() override
    {
        for (auto& postingIterator : _postingIterators) {
            postingIterator.second->Reset();
        }
        _cursor = 0;
    }

    PostingIterator* Clone() const override
    {
        MultiSegmentPostingIterator* multiSegmentPostingIterator = new MultiSegmentPostingIterator();
        std::vector<PostingIteratorInfo> postingIters;
        for (auto& postingIter : _postingIterators) {
            std::shared_ptr<PostingIterator> postingIterClone =
                std::shared_ptr<PostingIterator>(postingIter.second->Clone());
            postingIters.push_back(std::make_pair(postingIter.first, postingIterClone));
        }
        multiSegmentPostingIterator->Init(postingIters, _segmentIdxs, _fileWriters);
        return multiSegmentPostingIterator;
    }

    matchvalue_t GetMatchValue() const override
    {
        assert(false);
        return matchvalue_t();
    }

    termpayload_t GetTermPayLoad()
    {
        if (_postingIterators[_cursor].second->GetTermMeta()) {
            return _postingIterators[_cursor].second->GetTermMeta()->GetPayload();
        }
        return -1;
    }

    size_t GetPostingLength() const override
    {
        size_t totalSize = 0;
        for (auto& postingIter : _postingIterators) {
            if (postingIter.second) {
                totalSize += postingIter.second->GetPostingLength();
            }
        }
        return totalSize;
    }

private:
    std::vector<PostingIteratorInfo> _postingIterators;
    std::vector<file_system::InterimFileWriterPtr> _fileWriters;
    std::vector<segmentid_t> _segmentIdxs;
    int64_t _cursor;
    int64_t _size;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
