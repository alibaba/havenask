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

namespace indexlib::index {

class TruncatePostingIterator : public PostingIterator
{
public:
    TruncatePostingIterator(const std::shared_ptr<PostingIterator>& evaluatorIter,
                            const std::shared_ptr<PostingIterator>& collectorIter);
    ~TruncatePostingIterator();

public:
    TermMeta* GetTermMeta() const override;

    docid64_t SeekDoc(docid64_t docId) override;

    index::ErrorCode SeekDocWithErrorCode(docid64_t docId, docid64_t& result) override;

    bool HasPosition() const override;

    index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos) override;
    void Unpack(TermMatchData& termMatchData) override;

    docpayload_t GetDocPayload() override;

    PostingIteratorType GetType() const override;

    void Reset() override;

    PostingIterator* Clone() const override;
    autil::mem_pool::Pool* GetSessionPool() const override { return nullptr; }

public:
    std::shared_ptr<PostingIterator> GetCurrentIterator() const { return _curIter; }

private:
    std::shared_ptr<PostingIterator> _evaluatorIter;
    std::shared_ptr<PostingIterator> _collectorIter;
    std::shared_ptr<PostingIterator> _curIter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
