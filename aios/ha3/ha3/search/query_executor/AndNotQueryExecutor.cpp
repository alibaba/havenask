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
#include "ha3/search/AndNotQueryExecutor.h"

#include <cstddef>

#include "indexlib/index/inverted_index/DocValueFilter.h"

#include "ha3/isearch.h"
#include "autil/Log.h"

#include "ha3/search/ExecutorVisitor.h"

using namespace std;
namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, AndNotQueryExecutor);

AndNotQueryExecutor::AndNotQueryExecutor() 
    : _leftQueryExecutor(NULL)
    , _rightQueryExecutor(NULL)
    , _rightFilter(NULL)
    , _testDocCount(0)
    , _rightExecutorHasSub(false)
{
}

AndNotQueryExecutor::~AndNotQueryExecutor() { 
}

void AndNotQueryExecutor::accept(ExecutorVisitor *visitor) const {
    visitor->visitAndNotExecutor(this);
}

indexlib::index::ErrorCode AndNotQueryExecutor::doSeek(docid_t id, docid_t& result) {
    docid_t docId = INVALID_DOCID;
    auto ec = _leftQueryExecutor->seek(id, docId);
    IE_RETURN_CODE_IF_ERROR(ec);
    docid_t tmpId = INVALID_DOCID;
    while (_rightQueryExecutor && docId != END_DOCID) { 
        if (_rightFilter) {
            _testDocCount++;
            if (_rightFilter->Test(docId)) {
                tmpId = docId;
            } else {
                tmpId = END_DOCID;
            }
        } else {
            auto ec = _rightQueryExecutor->seek(docId, tmpId);
            IE_RETURN_CODE_IF_ERROR(ec);
        }

        if (_rightExecutorHasSub) {
            break;
        } else if (docId == tmpId) {
            auto ec = _leftQueryExecutor->seek(docId + 1, docId);
            IE_RETURN_CODE_IF_ERROR(ec);
        } else {
            break;
        }
    }
    result = docId;
    return indexlib::index::ErrorCode::OK;
}

indexlib::index::ErrorCode AndNotQueryExecutor::seekSubDoc(docid_t id, docid_t subDocId,
        docid_t subDocEnd, bool needSubMatchdata, docid_t& result)
{
    // right query must not contain current doc for now.
    do {
        auto ec = _leftQueryExecutor->seekSub(id, subDocId, subDocEnd,
                needSubMatchdata, subDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (subDocId >= subDocEnd) {
            result = END_DOCID;
            return indexlib::index::ErrorCode::OK;
        }
        docid_t rightDocId = INVALID_DOCID;
        ec = _rightQueryExecutor->seekSub(id, subDocId, subDocEnd,
                needSubMatchdata, rightDocId);
        IE_RETURN_CODE_IF_ERROR(ec);
        if (rightDocId == subDocId) {
            ec = _leftQueryExecutor->seekSub(id, subDocId + 1, subDocEnd,
                    needSubMatchdata, subDocId);
            IE_RETURN_CODE_IF_ERROR(ec);
        } else {
            break;
        }
    } while (subDocId < subDocEnd);
    result = subDocId;
    return indexlib::index::ErrorCode::OK;
}

void AndNotQueryExecutor::addQueryExecutors(
        QueryExecutor* leftExecutor,
        QueryExecutor* rightExecutor)
{
     
    _leftQueryExecutor = leftExecutor;
    _hasSubDocExecutor = leftExecutor->hasSubDocExecutor();
    _rightQueryExecutor = rightExecutor;
    _rightExecutorHasSub = rightExecutor->hasSubDocExecutor();
    if (_rightQueryExecutor && _leftQueryExecutor &&
        _rightQueryExecutor->getCurrentDF() >
        _leftQueryExecutor->getCurrentDF())
    {
        _rightFilter = rightExecutor->stealFilter();
    }
    // add all query executor to multi query executor for deconstruct,unpack,getMetaInfo.
    _queryExecutors.push_back(_leftQueryExecutor);
    if (_rightQueryExecutor) {
        _hasSubDocExecutor = _hasSubDocExecutor || _rightQueryExecutor->hasSubDocExecutor();
        _queryExecutors.push_back(_rightQueryExecutor);
    }
    if (_rightQueryExecutor) {
        _rightExecutorHasSub = _rightQueryExecutor->hasSubDocExecutor();
    }
}

bool AndNotQueryExecutor::isMainDocHit(docid_t docId) const {
    if (!_hasSubDocExecutor) {
        return true;
    }
    if (_rightExecutorHasSub) {
        return false;
    }
    return _leftQueryExecutor->isMainDocHit(docId);
}

df_t AndNotQueryExecutor::getDF(GetDFType type) const {
    return _queryExecutors[0]->getDF(type);
}

string AndNotQueryExecutor::toString() const {
    string ret = "ANDNOT(" + _leftQueryExecutor->toString();
    if (_rightQueryExecutor) {
        ret += "," + _rightQueryExecutor->toString();
    }
    ret += ")";
    return ret;
}

void AndNotQueryExecutor::setCurrSub(docid_t docid) {
    QueryExecutor::setCurrSub(docid);
    _leftQueryExecutor->setCurrSub(docid);
}

} // namespace search
} // namespace isearch

