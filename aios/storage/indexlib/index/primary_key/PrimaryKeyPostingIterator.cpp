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
#include "indexlib/index/primary_key/PrimaryKeyPostingIterator.h"

#include "indexlib/index/inverted_index/TermMatchData.h"
#include "indexlib/index/primary_key/PrimaryKeyInDocPositionState.h"
#include "indexlib/util/PoolUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PrimaryKeyPostingIterator);

PrimaryKeyPostingIterator::PrimaryKeyPostingIterator(docid_t docId, autil::mem_pool::Pool* sessionPool)
    : _docId(docId)
    , _isSearched(false)
    , _sessionPool(sessionPool)
{
    _termMeta = new TermMeta(1, 1);
}

PrimaryKeyPostingIterator::~PrimaryKeyPostingIterator() { DELETE_AND_SET_NULL(_termMeta); }

TermMeta* PrimaryKeyPostingIterator::GetTermMeta() const { return _termMeta; }

docid_t PrimaryKeyPostingIterator::SeekDoc(docid_t docId)
{
    if (_isSearched) {
        return INVALID_DOCID;
    } else {
        _isSearched = true;
        return (_docId >= docId) ? _docId : INVALID_DOCID;
    }
}

void PrimaryKeyPostingIterator::Unpack(TermMatchData& termMatchData)
{
    static PrimaryKeyInDocPositionState DUMMY_STATE;
    termMatchData.SetHasDocPayload(false);
    return termMatchData.SetInDocPositionState(&DUMMY_STATE);
}

PostingIterator* PrimaryKeyPostingIterator::Clone() const
{
    PrimaryKeyPostingIterator* iter =
        IE_POOL_COMPATIBLE_NEW_CLASS(_sessionPool, PrimaryKeyPostingIterator, _docId, _sessionPool);
    iter->_isSearched = false;
    return (PostingIterator*)iter;
}

void PrimaryKeyPostingIterator::Reset() { _isSearched = false; }
} // namespace indexlib::index
