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
#include "ha3/search/WeakAndQueryExecutor.h"

#include <assert.h>
#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"

#include "ha3/isearch.h"
#include "ha3/search/MultiQueryExecutor.h"
#include "ha3/search/QueryExecutorHeap.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, WeakAndQueryExecutor);

WeakAndQueryExecutor::WeakAndQueryExecutor(uint32_t minShouldMatch)
    : _count(0)
{
    _minShouldMatch = max(1u, minShouldMatch);
}

WeakAndQueryExecutor::~WeakAndQueryExecutor() {
}

void WeakAndQueryExecutor::doNthElement(std::vector<QueryExecutorEntry>& heap) {
    // sort minShoudMatch elements
    uint32_t i = 1;
    for (; i < _minShouldMatch; ++i) {
        if (heap[i].docId > heap[i+1].docId) {
            swap(heap[i], heap[i+1]);
        } else {
            break;
        }
    }
    if (i < _minShouldMatch) {
        return;
    }
    assert(i == _minShouldMatch);
    assert(_count >= _minShouldMatch);
    // heap adjustDown
    adjustDown(1, _count-_minShouldMatch+1, heap.data()+_minShouldMatch-1);
}

indexlib::index::ErrorCode WeakAndQueryExecutor::doSeek(docid_t id, docid_t& result)
{
    docid_t docId = INVALID_DOCID;
    do {
        auto ec = getQueryExecutor(_sortNHeap[1].entryId)->seek(id, docId);
        IE_RETURN_CODE_IF_ERROR(ec);
        _sortNHeap[1].docId = docId;
        doNthElement(_sortNHeap);
        if (id > _sortNHeap[1].docId) {
            continue;
        }
        id = _sortNHeap[_minShouldMatch].docId;
        if (_sortNHeap[1].docId == _sortNHeap[_minShouldMatch].docId) {
            break;
        }
    } while (END_DOCID != id);
    if (unlikely(END_DOCID == id)) {
        moveToEnd();
    }
    result = id;
    return indexlib::index::ErrorCode::OK;
}

void WeakAndQueryExecutor::reset() {
    MultiQueryExecutor::reset();
    addQueryExecutors(_queryExecutors);
}

indexlib::index::ErrorCode WeakAndQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    if (!_hasSubDocExecutor) {
        docid_t currSubDocId = END_DOCID;
        if (getDocId() == docId && subDocId < subDocEnd) {
            currSubDocId = subDocId;
        }
        if (unlikely(needSubMatchdata)) {
            setCurrSub(currSubDocId);
        }
        result = currSubDocId;
        return indexlib::index::ErrorCode::OK;
    }
    docid_t minSubDocId = END_DOCID;
    do {
        auto queryExecutor = getQueryExecutor(_sortNHeapSub[1].entryId);
        auto ec = queryExecutor->seekSub(docId, subDocId, END_DOCID, needSubMatchdata, minSubDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        _sortNHeapSub[1].docId = minSubDocId;
        doNthElement(_sortNHeapSub);
        if (subDocId > _sortNHeapSub[1].docId) {
            continue;
        }
        subDocId = _sortNHeapSub[_minShouldMatch].docId;
        if (_sortNHeapSub[1].docId == _sortNHeapSub[_minShouldMatch].docId) {
            break;
        }
    } while (subDocId < subDocEnd);
    if (subDocId >= subDocEnd) {
        result = END_DOCID;
        return indexlib::index::ErrorCode::OK;
    }
    result = subDocId;
    return indexlib::index::ErrorCode::OK;    
}

void WeakAndQueryExecutor::addQueryExecutors(const vector<QueryExecutor*> &queryExecutors) {
    _hasSubDocExecutor = false;
    _queryExecutors = queryExecutors;
    _sortNHeap.clear();
    QueryExecutorEntry dumyEntry;
    _sortNHeap.push_back(dumyEntry);
    _sortNHeapSub.push_back(dumyEntry);
    _count = 0;
    for (size_t i = 0; i < queryExecutors.size(); ++i) {
        _hasSubDocExecutor = _hasSubDocExecutor || queryExecutors[i]->hasSubDocExecutor();
        QueryExecutorEntry newEntry;
        newEntry.docId = INVALID_DOCID;
        newEntry.entryId = _count ++;
        _sortNHeap.push_back(newEntry);
        _sortNHeapSub.push_back(newEntry);
    }
    assert(_count > 0);
    _minShouldMatch = min(_minShouldMatch, _count);
}

bool WeakAndQueryExecutor::isMainDocHit(docid_t docId) const {
    if (!_hasSubDocExecutor) {
        return true;
    }
    const QueryExecutorVector &queryExecutors = getQueryExecutors();
    for (QueryExecutorVector::const_iterator it = queryExecutors.begin();
         it != queryExecutors.end(); ++it)
    {
        if(!(*it)->isMainDocHit(docId)) {
            return false;
        }
    }
    return true;
}

df_t WeakAndQueryExecutor::getDF(GetDFType type) const {
    df_t sumDF = 0;
    for (size_t i = 0; i < _queryExecutors.size(); ++i) {
        sumDF += _queryExecutors[i]->getDF(type);
    }
    return sumDF;
}

string WeakAndQueryExecutor::toString() const {
    return autil::StringUtil::toString(_minShouldMatch) +
        "WEAKAND" + MultiQueryExecutor::toString();
}

} // namespace search
} // namespace isearch
