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
#ifndef __INDEXLIB_TRUNCATE_POSTING_ITERATOR_H
#define __INDEXLIB_TRUNCATE_POSTING_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/indexlib.h"

namespace indexlib::index::legacy {

class TruncatePostingIterator : public PostingIterator
{
public:
    TruncatePostingIterator(const std::shared_ptr<PostingIterator>& evaluatorIter,
                            const std::shared_ptr<PostingIterator>& collectorIter);
    ~TruncatePostingIterator();

public:
    TermMeta* GetTermMeta() const override;

    docid_t SeekDoc(docid_t docId) override;

    index::ErrorCode SeekDocWithErrorCode(docid_t docId, docid_t& result) override;

    bool HasPosition() const override;

    index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextpos) override;
    void Unpack(TermMatchData& termMatchData) override;

    docpayload_t GetDocPayload() override;

    PostingIteratorType GetType() const override;

    void Reset() override;

    PostingIterator* Clone() const override;
    autil::mem_pool::Pool* GetSessionPool() const override { return NULL; }

public:
    std::shared_ptr<PostingIterator> GetCurrentIterator() const { return mCurIter; }

private:
    std::shared_ptr<PostingIterator> mEvaluatorIter;
    std::shared_ptr<PostingIterator> mCollectorIter;
    std::shared_ptr<PostingIterator> mCurIter;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncatePostingIterator);
} // namespace indexlib::index::legacy

#endif //__INDEXLIB_TRUNCATE_POSTING_ITERATOR_H
