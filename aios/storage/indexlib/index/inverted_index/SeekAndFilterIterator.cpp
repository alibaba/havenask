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
#include "indexlib/index/inverted_index/SeekAndFilterIterator.h"

#include "indexlib/util/PoolUtil.h"

using namespace std;

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, SeekAndFilterIterator);

SeekAndFilterIterator::SeekAndFilterIterator(PostingIterator* indexSeekIterator, DocValueFilter* docFilter,
                                             autil::mem_pool::Pool* sessionPool)
    : _indexSeekIterator(indexSeekIterator)
    , _docFilter(docFilter)
    , _sessionPool(sessionPool)
    , _needInnerFilter(false)
{
    assert(_indexSeekIterator);

    // currently: only spatial index not recall precisely, (date ?)
    _needInnerFilter = (indexSeekIterator->GetType() == pi_spatial);
}

SeekAndFilterIterator::~SeekAndFilterIterator()
{
    IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _indexSeekIterator);
    if (_docFilter) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _docFilter);
    }
}

PostingIterator* SeekAndFilterIterator::Clone() const
{
    assert(_indexSeekIterator);
    SeekAndFilterIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, SeekAndFilterIterator, _indexSeekIterator->Clone(),
                                     _docFilter ? _docFilter->Clone() : NULL, _sessionPool);
    assert(iter);
    return iter;
}

docid64_t SeekAndFilterIterator::SeekDoc(docid64_t docid)
{
    if (!_needInnerFilter || !_docFilter) {
        return _indexSeekIterator->SeekDoc(docid);
    }
    docid64_t curDocId = docid;
    while (true) {
        curDocId = _indexSeekIterator->SeekDoc(curDocId);
        if (curDocId == INVALID_DOCID) {
            break;
        }
        if (_docFilter->Test(curDocId)) {
            return curDocId;
        }
        ++curDocId;
    }
    return INVALID_DOCID;
}

index::ErrorCode SeekAndFilterIterator::SeekDocWithErrorCode(docid64_t docId, docid64_t& result)
{
    {
        if (!_needInnerFilter || !_docFilter) {
            return _indexSeekIterator->SeekDocWithErrorCode(docId, result);
        }
        docid64_t curDocId = docId;
        while (true) {
            auto ec = _indexSeekIterator->SeekDocWithErrorCode(curDocId, curDocId);
            if (unlikely(ec != index::ErrorCode::OK)) {
                return ec;
            }
            if (curDocId == INVALID_DOCID) {
                break;
            }
            if (_docFilter->Test(curDocId)) {
                result = curDocId;
                return index::ErrorCode::OK;
            }
            ++curDocId;
        }
        result = INVALID_DOCID;
        return index::ErrorCode::OK;
    }
}
} // namespace indexlib::index
