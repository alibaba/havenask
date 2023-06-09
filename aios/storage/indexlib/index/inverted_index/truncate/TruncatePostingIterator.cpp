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
#include "indexlib/index/inverted_index/truncate/TruncatePostingIterator.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, TruncatePostingIterator);

TruncatePostingIterator::TruncatePostingIterator(const std::shared_ptr<PostingIterator>& evaluatorIter,
                                                 const std::shared_ptr<PostingIterator>& collectorIter)
    : _evaluatorIter(evaluatorIter)
    , _collectorIter(collectorIter)
    , _curIter(evaluatorIter)
{
}

TruncatePostingIterator::~TruncatePostingIterator() {}

TermMeta* TruncatePostingIterator::GetTermMeta() const { return _curIter->GetTermMeta(); }

docid_t TruncatePostingIterator::SeekDoc(docid_t docId) { return _curIter->SeekDoc(docId); }

index::ErrorCode TruncatePostingIterator::SeekDocWithErrorCode(docid_t docId, docid_t& result)
{
    return _curIter->SeekDocWithErrorCode(docId, result);
}

bool TruncatePostingIterator::HasPosition() const { return _curIter->HasPosition(); }

index::ErrorCode TruncatePostingIterator::SeekPositionWithErrorCode(pos_t pos, pos_t& result)
{
    return _curIter->SeekPositionWithErrorCode(pos, result);
}

void TruncatePostingIterator::Unpack(TermMatchData& termMatchData) { _curIter->Unpack(termMatchData); }

docpayload_t TruncatePostingIterator::GetDocPayload() { return _curIter->GetDocPayload(); }

PostingIteratorType TruncatePostingIterator::GetType() const { return _curIter->GetType(); }

void TruncatePostingIterator::Reset()
{
    _curIter = _collectorIter;
    _curIter->Reset();
}

PostingIterator* TruncatePostingIterator::Clone() const
{
    std::shared_ptr<PostingIterator> iter1(_evaluatorIter->Clone());
    std::shared_ptr<PostingIterator> iter2(_collectorIter->Clone());
    TruncatePostingIterator* iter = new TruncatePostingIterator(iter1, iter2);
    return iter;
}
} // namespace indexlib::index
