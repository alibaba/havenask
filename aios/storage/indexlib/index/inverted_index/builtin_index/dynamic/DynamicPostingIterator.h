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
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicSearchTree.h"
#include "indexlib/index/inverted_index/format/TermMeta.h"

namespace indexlib::index {

class DynamicPostingIterator final : public PostingIterator
{
public:
    struct SegmentContext {
        docid_t baseDocId = INVALID_DOCID;
        uint32_t docCount = 0;
        const DynamicSearchTree* tree = nullptr;
    };

public:
    DynamicPostingIterator(autil::mem_pool::Pool* sessionPool, std::vector<SegmentContext> segmentContexts);
    ~DynamicPostingIterator();

public:
    TermMeta* GetTermMeta() const override;
    docid_t SeekDoc(docid_t docId) override;
    index::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;
    bool HasPosition() const override { return false; }
    void Unpack(TermMatchData& termMatchData) override { assert(false); }
    void Reset() override;
    PostingIterator* Clone() const override;
    PostingIteratorType GetType() const override { return pi_dynamic; }
    autil::mem_pool::Pool* GetSessionPool() const override { return _sessionPool; }

public:
    bool SeekDocWithDelete(docid_t docId, KeyType* result);

private:
    autil::mem_pool::Pool* _sessionPool;
    int64_t _segmentCursor;
    std::unique_ptr<DynamicSearchTree::Iterator> _currentIterator;
    std::vector<SegmentContext> _segmentContexts;
    TermMeta _termMeta;
    docid_t _currentDocId;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
