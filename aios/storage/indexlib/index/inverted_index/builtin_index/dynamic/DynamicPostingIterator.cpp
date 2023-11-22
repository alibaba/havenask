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
#include "indexlib/index/inverted_index/builtin_index/dynamic/DynamicPostingIterator.h"

#include "indexlib/util/PoolUtil.h"

namespace indexlib ::index {
AUTIL_LOG_SETUP(indexlib.index, DynamicPostingIterator);

DynamicPostingIterator::DynamicPostingIterator(autil::mem_pool::Pool* sessionPool,
                                               std::vector<SegmentContext> segmentContexts)
    : _sessionPool(sessionPool)
    , _segmentCursor(-1)
    , _segmentContexts(std::move(segmentContexts))
    , _currentDocId(INVALID_DOCID)
{
    size_t df = 0;
    for (const auto& segCtx : _segmentContexts) {
        df += segCtx.tree->EstimateDocCount();
    }
    _termMeta.SetDocFreq(df);
}

DynamicPostingIterator::~DynamicPostingIterator() {}

PostingIterator* DynamicPostingIterator::Clone() const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, DynamicPostingIterator, _sessionPool, _segmentContexts);
}

void DynamicPostingIterator::Reset()
{
    _segmentCursor = -1;
    _currentIterator.reset();
    _currentDocId = INVALID_DOCID;
}

TermMeta* DynamicPostingIterator::GetTermMeta() const { return const_cast<TermMeta*>(&_termMeta); }

docid64_t DynamicPostingIterator::SeekDoc(docid64_t docId)
{
    docid64_t docRet = INVALID_DOCID;
    auto ec = SeekDocWithErrorCode(docId, docRet);
    index::ThrowIfError(ec);
    return docRet;
}

index::ErrorCode DynamicPostingIterator::SeekDocWithErrorCode(docid64_t docId, docid64_t& result)
{
    KeyType keyResult;
    for (;;) {
        bool r = SeekDocWithDelete(docId, &keyResult);
        if (!r) {
            result = INVALID_DOCID;
            return index::ErrorCode::OK;
        }
        if (keyResult.IsDelete()) {
            continue;
        }
        result = keyResult.DocId();
        return index::ErrorCode::OK;
    }
    return index::ErrorCode::InconsistentState;
}

bool DynamicPostingIterator::SeekDocWithDelete(docid64_t docId, KeyType* result)
{
    docId = std::max(docId, _currentDocId + 1);
    for (;;) {
        if (!_currentIterator) {
            ++_segmentCursor;
            if (_segmentCursor >= _segmentContexts.size()) {
                return false;
            }
            const auto& ctx = _segmentContexts[_segmentCursor];
            auto tree = ctx.tree;
            if (ctx.baseDocId + tree->MaxDocId() < docId) {
                continue;
            }
            // docId < ctx.base + maxdoc

            _currentIterator = std::make_unique<DynamicSearchTree::Iterator>(tree->CreateIterator());
        }
        KeyType key;
        if (_currentIterator->Seek(docId - _segmentContexts[_segmentCursor].baseDocId, &key)) {
            *result = KeyType(key.DocId() + _segmentContexts[_segmentCursor].baseDocId, key.IsDelete());
            _currentDocId = key.DocId();
            return true;
        } else {
            _currentIterator.reset();
            continue;
        }
    }
    return false;
}

} // namespace indexlib::index
