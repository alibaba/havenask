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
#include "ha3/search/OrQueryExecutor.h"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "ha3/isearch.h"
#include "ha3/search/ExecutorVisitor.h"
#include "ha3/search/MultiQueryExecutor.h"
#include "ha3/search/QueryExecutorHeap.h"
#include "ha3/search/TermMetaInfo.h"

using namespace std;
using namespace isearch::rank;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, OrQueryExecutor);

OrQueryExecutor::OrQueryExecutor() {
    _count = 0;    
}

OrQueryExecutor::~OrQueryExecutor() { 
}

void OrQueryExecutor::accept(ExecutorVisitor *visitor) const {
    visitor->visitOrExecutor(this);
}

indexlib::index::ErrorCode OrQueryExecutor::doSeek(docid_t id, docid_t& result) {
    docid_t docId = INVALID_DOCID;
    do {
        auto ec = getQueryExecutor(_qeMinHeap[1].entryId)->seek(id, docId);
        IE_RETURN_CODE_IF_ERROR(ec);
        _qeMinHeap[1].docId = docId;
        adjustDown(1, _count, _qeMinHeap.data());
    } while (id > _qeMinHeap[1].docId);
    result =  _qeMinHeap[1].docId;
    return indexlib::index::ErrorCode::OK;
}

void OrQueryExecutor::reset() {
    MultiQueryExecutor::reset();
    addQueryExecutors(_queryExecutors);
}

indexlib::index::ErrorCode OrQueryExecutor::seekSubDoc(docid_t docId, docid_t subDocId,
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
    const QueryExecutorVector &queryExecutors = getQueryExecutors();
    docid_t minSubDocId = END_DOCID;
    for (QueryExecutorVector::const_iterator it = queryExecutors.begin();
         it != queryExecutors.end(); ++it)
    {
        docid_t ret = INVALID_DOCID;
        auto ec = (*it)->seekSub(docId, subDocId, subDocEnd, needSubMatchdata, ret);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (ret < minSubDocId) {
            minSubDocId = ret;
        }
    }
    result = minSubDocId;
    return indexlib::index::ErrorCode::OK;
}

void OrQueryExecutor::addQueryExecutors(const vector<QueryExecutor*> &queryExecutors) {
    _hasSubDocExecutor = false;
    _queryExecutors = queryExecutors;
    _qeMinHeap.clear();
    QueryExecutorEntry dumyEntry;
    _qeMinHeap.push_back(dumyEntry);
    _count = 0;
    for (size_t i = 0; i < queryExecutors.size(); ++i) {
        _hasSubDocExecutor = _hasSubDocExecutor || queryExecutors[i]->hasSubDocExecutor();
        QueryExecutorEntry newEntry;
        newEntry.docId = INVALID_DOCID;
        newEntry.entryId = _count ++;
        _qeMinHeap.push_back(newEntry);
    }
}

bool OrQueryExecutor::isMainDocHit(docid_t docId) const {
    if (!_hasSubDocExecutor) {
        return true;
    }
    const QueryExecutorVector &queryExecutors = getQueryExecutors();
    for (QueryExecutorVector::const_iterator it = queryExecutors.begin();
         it != queryExecutors.end(); ++it)
    {
        if((*it)->getDocId() == docId && (*it)->isMainDocHit(docId)) {
            return true;
        }
    }
    return false;
}


df_t OrQueryExecutor::getDF(GetDFType type) const {
    df_t sumDF = 0;
    for (size_t i = 0; i < _queryExecutors.size(); ++i) {
        sumDF += _queryExecutors[i]->getDF(type);
    }
    return sumDF;
}

string OrQueryExecutor::toString() const {
    return "OR" + MultiQueryExecutor::toString();
}

} // namespace search
} // namespace isearch

