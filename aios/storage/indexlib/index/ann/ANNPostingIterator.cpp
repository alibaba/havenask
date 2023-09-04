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
#include "indexlib/index/ann/ANNPostingIterator.h"

#include "indexlib/util/PathUtil.h"

using namespace indexlib;
using namespace autil::mem_pool;
namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, ANNPostingIterator);

ANNPostingIterator::ANNPostingIterator(Pool* sessionPool)
    : _docIds(nullptr)
    , _matchValues(nullptr)
    , _matchCount(0)
    , _sessionPool(sessionPool)
    , _cursor(-1)
    , _curDocId(INVALID_DOCID)
    , _curMatchValue(matchvalue_t())
    , _termMeta(nullptr)
{
}

ANNPostingIterator::ANNPostingIterator(const std::vector<ANNMatchItem>& annMatchItems,
                                       autil::mem_pool::Pool* sessionPool)
    : _matchCount(annMatchItems.size())
    , _sessionPool(sessionPool)
    , _cursor(-1)
    , _curDocId(INVALID_DOCID)
    , _curMatchValue(matchvalue_t())
{
    _docIds = IE_POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, docid_t, _matchCount);
    _matchValues = IE_POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, matchvalue_t, _matchCount);
    for (size_t i = 0; i < _matchCount; ++i) {
        _docIds[i] = annMatchItems[i].docid;
        _matchValues[i].SetFloat(annMatchItems[i].score);
    }
    _termMeta = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, indexlib::index::TermMeta, _matchCount, _matchCount, 0);
}

ANNPostingIterator::~ANNPostingIterator()
{
    if (nullptr != _docIds) {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _docIds, _matchCount);
    }
    if (nullptr != _matchValues) {
        IE_POOL_COMPATIBLE_DELETE_VECTOR(_sessionPool, _matchValues, _matchCount);
    }
    if (nullptr != _termMeta) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_sessionPool, _termMeta);
    }
}

docid_t ANNPostingIterator::SeekDoc(docid_t docId)
{
    if (0 == _matchCount) {
        return INVALID_DOCID;
    }
    docid_t ret = INVALID_DOCID;
    if (SeekDocWithErrorCode(docId, ret) != indexlib::index::ErrorCode::OK) {
        return INVALID_DOCID;
    }
    return ret;
}

indexlib::index::ErrorCode ANNPostingIterator::SeekDocWithErrorCode(docid_t docId, docid_t& result)
{
    if (0 == _matchCount) {
        result = INVALID_DOCID;
        return indexlib::index::ErrorCode::OK;
    }
    if (docId == _curDocId) {
        _cursor = _cursor + 1;
        if (_cursor < _matchCount) {
            _curDocId = _docIds[_cursor];
            _curMatchValue = _matchValues[_cursor];
            result = _curDocId;
        } else {
            _cursor = -1;
            _curDocId = INVALID_DOCID;
            _curMatchValue = matchvalue_t();
            result = INVALID_DOCID;
        }
    } else {
        for (size_t i = 0; i != _matchCount; ++i) {
            if (docId <= _docIds[i]) {
                _cursor = i;
                _curDocId = _docIds[_cursor];
                _curMatchValue = _matchValues[_cursor];
                result = _curDocId;
                break;
            }
        }
    }
    return indexlib::index::ErrorCode::OK;
}

indexlib::index::PostingIterator* ANNPostingIterator::Clone() const
{
    ANNPostingIterator* clonedItetator = IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, ANNPostingIterator, _sessionPool);
    clonedItetator->_docIds = IE_POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, docid_t, _matchCount);
    memcpy(clonedItetator->_docIds, _docIds, _matchCount * (sizeof(docid_t)));
    clonedItetator->_matchValues = IE_POOL_COMPATIBLE_NEW_VECTOR(_sessionPool, matchvalue_t, _matchCount);
    memcpy(clonedItetator->_matchValues, _matchValues, _matchCount * (sizeof(matchvalue_t)));
    clonedItetator->_matchCount = _matchCount;
    clonedItetator->_termMeta =
        IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, indexlib::index::TermMeta, _matchCount, _matchCount, 0);
    return clonedItetator;
}

void ANNPostingIterator::Reset()
{
    _cursor = -1;
    _curDocId = INVALID_DOCID;
    _curMatchValue = matchvalue_t();
}

} // namespace indexlibv2::index
