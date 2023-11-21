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

#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

namespace indexlib::index {

class PrimaryKeyPostingIterator : public PostingIterator
{
public:
    PrimaryKeyPostingIterator(docid64_t docId, autil::mem_pool::Pool* sessionPool = NULL);
    ~PrimaryKeyPostingIterator();

public:
    TermMeta* GetTermMeta() const override;
    docid64_t SeekDoc(docid64_t docId) override;
    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override
    {
        result = SeekDoc(docId);
        return index::ErrorCode::OK;
    }
    void Unpack(TermMatchData& termMatchData) override;
    PostingIteratorType GetType() const override { return pi_pk; }

    bool HasPosition() const override { return false; }

    PostingIterator* Clone() const override;
    void Reset() override;
    autil::mem_pool::Pool* GetSessionPool() const override { return _sessionPool; }

private:
    docid64_t _docId;
    bool _isSearched;
    autil::mem_pool::Pool* _sessionPool;
    TermMeta* _termMeta;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<PrimaryKeyPostingIterator> PrimaryKeyPostingIteratorPtr;
} // namespace indexlib::index
