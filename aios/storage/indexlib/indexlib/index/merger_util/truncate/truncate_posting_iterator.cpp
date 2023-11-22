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
#include "indexlib/index/merger_util/truncate/truncate_posting_iterator.h"

using namespace std;

namespace indexlib::index::legacy {
IE_LOG_SETUP(index, TruncatePostingIterator);

TruncatePostingIterator::TruncatePostingIterator(const std::shared_ptr<PostingIterator>& evaluatorIter,
                                                 const std::shared_ptr<PostingIterator>& collectorIter)
    : mEvaluatorIter(evaluatorIter)
    , mCollectorIter(collectorIter)
    , mCurIter(evaluatorIter)
{
}

TruncatePostingIterator::~TruncatePostingIterator() {}

TermMeta* TruncatePostingIterator::GetTermMeta() const { return mCurIter->GetTermMeta(); }

docid64_t TruncatePostingIterator::SeekDoc(docid64_t docId) { return mCurIter->SeekDoc(docId); }

index::ErrorCode TruncatePostingIterator::SeekDocWithErrorCode(docid64_t docId, docid64_t& result)
{
    return mCurIter->SeekDocWithErrorCode(docId, result);
}

bool TruncatePostingIterator::HasPosition() const { return mCurIter->HasPosition(); }

index::ErrorCode TruncatePostingIterator::SeekPositionWithErrorCode(pos_t pos, pos_t& result)
{
    return mCurIter->SeekPositionWithErrorCode(pos, result);
}

void TruncatePostingIterator::Unpack(TermMatchData& termMatchData) { mCurIter->Unpack(termMatchData); }

docpayload_t TruncatePostingIterator::GetDocPayload() { return mCurIter->GetDocPayload(); }

PostingIteratorType TruncatePostingIterator::GetType() const { return mCurIter->GetType(); }

void TruncatePostingIterator::Reset()
{
    mCurIter = mCollectorIter;
    mCurIter->Reset();
}

PostingIterator* TruncatePostingIterator::Clone() const
{
    std::shared_ptr<PostingIterator> iter1(mEvaluatorIter->Clone());
    std::shared_ptr<PostingIterator> iter2(mCollectorIter->Clone());
    TruncatePostingIterator* iter = new TruncatePostingIterator(iter1, iter2);
    return iter;
}
} // namespace indexlib::index::legacy
