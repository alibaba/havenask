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
#include "indexlib/index/inverted_index/builtin_index/spatial/SpatialPostingIterator.h"

#include "autil/memory.h"
#include "indexlib/index/inverted_index/TermMatchData.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SpatialPostingIterator);

SpatialPostingIterator::SpatialPostingIterator(autil::mem_pool::Pool* sessionPool)
    : _curDocid(INVALID_DOCID)
    , _termMeta(NULL)
    , _sessionPool(sessionPool)
    , _seekDocCounter(0)
{
}

SpatialPostingIterator::~SpatialPostingIterator()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _termMeta);
    for (size_t i = 0; i < _postingIterators.size(); i++) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _postingIterators[i]);
    }
}

ErrorCode SpatialPostingIterator::Init(const std::vector<BufferedPostingIterator*>& postingIterators)
{
    df_t docFreq = 0;
    tf_t totalTF = 0;
    _postingIterators = postingIterators;
    for (size_t i = 0; i < postingIterators.size(); i++) {
        TermMeta* termMeta = postingIterators[i]->GetTermMeta();
        docFreq += termMeta->GetDocFreq();
        totalTF += termMeta->GetTotalTermFreq();
        docid64_t docid = INVALID_DOCID;
        auto errorCode = postingIterators[i]->SeekDocWithErrorCode(INVALID_DOCID, docid);
        if (errorCode != ErrorCode::OK) {
            return errorCode;
        }
        if (docid != INVALID_DOCID) {
            _heap.push(std::make_pair(postingIterators[i], docid));
        }
    }
    _termMeta = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, TermMeta, docFreq, totalTF, 0);
    return ErrorCode::OK;
}

void SpatialPostingIterator::Unpack(TermMatchData& termMatchData)
{
    // do nothing because spatial index not support docpayload, fieldmap, tf
    termMatchData.SetMatched(true);
}

docid64_t SpatialPostingIterator::InnerSeekDoc(docid64_t docid)
{
    docid64_t result = 0;
    auto ec = InnerSeekDoc(docid, result);
    ThrowIfError(ec);
    return result;
}

ErrorCode SpatialPostingIterator::InnerSeekDoc(docid64_t docid, docid64_t& result)
{
    docid = std::max(_curDocid + 1, docid);
    docid64_t nextDocid = 0;
    while (!_heap.empty()) {
        _seekDocCounter++;
        auto [iter, heapDocId] = _heap.top();
        if (heapDocId >= docid) {
            _curDocid = heapDocId;
            result = _curDocid;
            return ErrorCode::OK;
        }
        _heap.pop();
        auto ec = iter->SeekDocWithErrorCode(docid, nextDocid);
        if (unlikely(ec != ErrorCode::OK)) {
            return ec;
        }
        if (nextDocid != INVALID_DOCID) {
            _heap.push(std::make_pair(iter, nextDocid));
        }
    }
    result = INVALID_DOCID;
    return ErrorCode::OK;
}

docid64_t SpatialPostingIterator::SeekDoc(docid64_t docid)
{
    // should not seek back
    return InnerSeekDoc(docid);
}

void SpatialPostingIterator::Reset()
{
    _curDocid = INVALID_DOCID;
    while (!_heap.empty()) {
        _heap.pop();
    }

    for (auto iter : _postingIterators) {
        iter->Reset();
        docid64_t docid = iter->SeekDoc(INVALID_DOCID);
        if (docid != INVALID_DOCID) {
            _heap.push(std::make_pair(iter, docid));
        }
    }
}

SpatialPostingIterator* SpatialPostingIterator::Clone() const
{
    auto iter = autil::unique_ptr_pool_deallocated(
        _sessionPool, IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SpatialPostingIterator, _sessionPool));
    assert(iter != nullptr);
    decltype(_postingIterators) newPostingIterators;
    for (auto iter : _postingIterators) {
        newPostingIterators.push_back(iter->Clone());
    }
    if (auto ec = iter->Init(newPostingIterators); ec != ErrorCode::OK) {
        index::ThrowIfError(ec);
    }
    return iter.release();
}

} // namespace indexlib::index
