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

#include "indexlib/index/inverted_index/DocValueFilter.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/RangePostingIterator.h"

namespace indexlib::index {

class SeekAndFilterIterator : public PostingIterator
{
public:
    SeekAndFilterIterator(PostingIterator* indexSeekIterator, DocValueFilter* docFilter,
                          autil::mem_pool::Pool* sessionPool);
    ~SeekAndFilterIterator();

public:
    PostingIteratorType GetType() const override { return pi_seek_and_filter; }
    indexlib::index::InvertedIndexSearchTracer* GetSearchTracer() const override
    {
        return _indexSeekIterator->GetSearchTracer();
    }
    PostingIteratorType GetInnerIteratorType() const { return _indexSeekIterator->GetType(); }

    DocValueFilter* GetDocValueFilter() const { return _docFilter; }
    PostingIterator* GetIndexIterator() const { return _indexSeekIterator; }

private:
    TermMeta* GetTermMeta() const override { return _indexSeekIterator->GetTermMeta(); }
    TermMeta* GetTruncateTermMeta() const override { return _indexSeekIterator->GetTruncateTermMeta(); }
    TermPostingInfo GetTermPostingInfo() const override { return _indexSeekIterator->GetTermPostingInfo(); }
    autil::mem_pool::Pool* GetSessionPool() const override { return _sessionPool; }

    docid64_t SeekDoc(docid64_t docid) override;
    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override;
    docpayload_t GetDocPayload() override { return _indexSeekIterator->GetDocPayload(); }
    bool HasPosition() const override { return _indexSeekIterator->HasPosition(); }
    // TODO: support
    void Unpack(TermMatchData& termMatchData) override { _indexSeekIterator->Unpack(termMatchData); }
    PostingIterator* Clone() const override;
    void Reset() override { _indexSeekIterator->Reset(); }

private:
    PostingIterator* _indexSeekIterator;
    DocValueFilter* _docFilter;
    autil::mem_pool::Pool* _sessionPool;
    bool _needInnerFilter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
